#!/usr/bin/env ruby

require 'bundler/inline'

gemfile do
  source 'https://rubygems.org'
  gem 'spark_api', '1.4.29'
  gem 'nokogiri', '1.10.4'
end

# set up session and RESO Web API middleware
SparkApi.configure do |config|
    config.authentication_mode = SparkApi::Authentication::OAuth2
    config.middleware = :reso_api
    config.version = "v1"
    config.endpoint = "https://replication.sparkapi.com"
    config.timeout = 45 # extending default timeout, for our large requests
end

SparkApi.client.session = SparkApi::Authentication::OAuthSession.new({ :access_token => "YOUR_ACCESS_TOKEN_HERE" })

# pull metadata from RESO Web API
metadata_res = (SparkApi.client.get("/$metadata"))
metadata_xml = Nokogiri::XML(metadata_res).remove_namespaces!

# make an array of fields which need to be checked for readable values
fields_to_lookup = []
metadata_xml.xpath("//Schema/EnumType/@Name").each do |el|
  fields_to_lookup << el.to_str
end
# puts fields_to_lookup

def swap_readable_values(listings, fields_to_lookup, metadata_xml)
  # Make the human-readable substitutions
  listings["value"].each do |listing| # for each listing,
    fields_to_lookup.each do |field| # go through the array of fields to be checked.
      if !!listing[field] # when one of the fields that needs to be checked exists in a listing,
        if listing[field].is_a? String
          readable = metadata_xml.xpath( # check for readable value to be swapped in
            "//Schema/
            EnumType[@Name=\"#{field}\"]/
            Member[@Name=\"#{listing[field]}\"]/
            Annotation"
          ).attr("String")

          # if there is a readable value, swap it in
          if !!readable
            listing[field] = readable.to_str
          end

        elsif listing[field].is_a? Array
          readable_arr = []
          listing[field].each do |el|
            readable = metadata_xml.xpath( # check for readable value to be swapped in
              "//Schema/
              EnumType[@Name=\"#{field}\"]/
              Member[@Name=\"#{el}\"]/
              Annotation"
            ).attr("String")

            # assemble a new array with readable values and swap it in
            if !!readable
              readable_arr << readable.to_str
            else
              readable_arr << el
            end
            listing[field] = readable_arr
          end
        end

      end
    end

    # handle MediaCategory
    listing["Media"].each do |media_rec|
      media_category = media_rec["MediaCategory"]
      if media_category.slice(0,3) == "b__"
        readable = metadata_xml.xpath( # check for readable value to be swapped in
          "//Schema/
          EnumType[@Name=\"MediaCategory\"]/
          Member[@Name=\"#{media_category}\"]/
          Annotation"
        ).attr("String")
        if !!readable
          media_rec["MediaCategory"] = readable
        end
      end
    end

    # handle CustomFields
    new_customs = {} # build a new hash to swap in
    custom_fields = listing["CustomFields"][0]
    custom_fields.each do |key, value|
      if key.slice(0,3) == "b__"
        readable = metadata_xml.xpath( # check for readable value to be swapped in
          "//Schema/
          EntityType[@Name=\"CustomFields\"]/
          Property[@Name=\"#{key}\"]/
          Annotation"
        ).attr("String").to_str
        if !!readable
          new_customs[readable] = value
        end
      else
        new_customs[key] = value
      end
    end
    listing["CustomFields"][0] = new_customs

  end
end

# determine run mode based on whether listings.json exists or not
if File.file?("listings.json")
  run_mode = "update_recent"
  puts "Run mode: update"
else
  run_mode = "initial_replication"
  puts "Run mode: initial replication"
end

# read in last updated time...
last_updated = nil # properly scope variable
if File.file?("last_updated.txt")
  file = File.open("last_updated.txt", "r").each do |line|
    last_updated = line.chomp
  end
  file.close
end
# ...then overwrite last_updated.txt with current time
now = Time.now - 1200 # subtract 20m to account for server side caching
now_utc = now.getutc.iso8601
file = File.open("last_updated.txt", "w")
file.puts now_utc
file.close

if run_mode == "initial_replication"
  # small initial request to determine how many 1000-listing requests are needed to receive all listings
  initial_request = (SparkApi.client.get("/Property", {
                                           :$top => 1,
                                           :$count => true,
                                           :$select => "ListingKey"
                                         }))
  number_of_records = initial_request["@odata.count"]
  puts "Total number of records: " + number_of_records.to_s

  puts "Enter the number of listings to pull per API call (max. 1000):"
  records_per_call = gets.chomp.to_i

  if (number_of_records % records_per_call) == 0
    number_of_requests = (number_of_records / records_per_call)
  else
    number_of_requests = (number_of_records / records_per_call) + 1
  end
  puts "Number of requests: " + number_of_requests.to_s

  results = []
  skiptoken = "" # first request with a blank skiptoken

  number_of_requests.times { |index|
    puts "Making request number #{index + 1}..."
    listings = (SparkApi.client.get("/Property", {
                                      :$top => records_per_call,
                                      :$expand=>"Media,CustomFields",
                                      :$skiptoken => skiptoken
                                    }))

    puts "API call complete"
    unless listings["@odata.nextLink"].nil? # the last page of results does not return a skiptoken
      skiptoken = CGI.parse(URI.parse(listings["@odata.nextLink"]).query)["$skiptoken"][0]
      puts "$skiptoken for next request: #{skiptoken}"
    else
      puts "Requests complete"
    end

    puts "Swapping encoded values for readable ones"
    swap_readable_values(listings, fields_to_lookup, metadata_xml)

    # note: concatenating many responses into a single array before writing out has the potential to cause memory issues
    results += listings["value"]
  }

  file = File.open("listings.json", "w")
  file.puts results.to_json
  file.close
  puts "Created file 'listings.json'"
end

if run_mode == 'update_recent'
  # small initial request to determine how many 1000-listing requests are needed to update recently modified listings
  initial_request = (SparkApi.client.get("/Property", {
                                           :$top => 1,
                                           :$count => true,
                                           :$filter => "ModificationTimestamp gt #{last_updated}",
                                           :$select => 'ListingKey'
                                         }))
  number_of_records = initial_request["@odata.count"]
  puts "Number of new or updated records: " + number_of_records.to_s

  puts "Enter the number of listings to pull per API call (max. 1000):"
  records_per_call = gets.chomp.to_i

  if (number_of_records % records_per_call) == 0
   number_of_requests = (number_of_records / records_per_call)
  else
   number_of_requests = (number_of_records / records_per_call) + 1
  end
  puts "Number of requests: " + number_of_requests.to_s

  results = []
  skiptoken = "" # first request without skiptoken
  number_of_requests.times { |index|
    puts "Making request number #{index + 1}..."
    listings = (SparkApi.client.get("/Property", {
                                      :$top => records_per_call,
                                      :$filter => "ModificationTimestamp gt #{last_updated}",
                                      :$expand=>"Media,CustomFields",
                                      :$skiptoken => skiptoken
                                   }))
    puts "API call complete"
    unless listings["@odata.nextLink"].nil? # the last page of results does not return a skiptoken
      skiptoken = CGI.parse(URI.parse(listings["@odata.nextLink"]).query)["$skiptoken"][0]
      puts "$skiptoken for next request: #{skiptoken}"
    else
      puts "Requests complete"
    end

    puts "Swapping encoded values for readable ones"
    swap_readable_values(listings, fields_to_lookup, metadata_xml)

    results += listings["value"]
  }

  # update results
  file = File.open("updated_listings_#{now_utc}.json", "w")
  file.puts results.to_json
  file.close
  puts "Created file 'updated_listings_#{now_utc}.json'"
end
