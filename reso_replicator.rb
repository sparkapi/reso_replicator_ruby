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
    config.timeout = 30 # extending default timeout, for our large requests
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
  end
end

# determine run mode based on whether listings.json exists or not
if File.file?("listings.json")
  run_mode = "update_recent"
else
  run_mode = "initial_replication"
end
puts "run mode: #{run_mode}"

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
  puts "number_of_records: " + number_of_records.to_s
  if (number_of_records % 1000) == 0
    number_of_requests = (number_of_records / 1000)
  else
    number_of_requests = (number_of_records / 1000) + 1
  end
  puts "number_of_requests: " + number_of_requests.to_s

  results = []
  skiptoken = "" # first request with a blank skiptoken
  number_of_requests.times {
    listings = (SparkApi.client.get("/Property", {
                                      :$top => 1000,
                                      :$expand=>"Media,CustomFields",
                                      :$skiptoken => skiptoken
                                    }))

    swap_readable_values(listings, fields_to_lookup, metadata_xml)

    # note: concatenating many responses into a single array before writing out has the potential to cause memory issues
    results += listings["value"]
    unless listings["@odata.nextLink"].nil? # the last page of results does not return a skiptoken
      skiptoken = CGI.parse(URI.parse(listings["@odata.nextLink"]).query)["$skiptoken"][0]
      puts "skiptoken for next request: #{skiptoken}"
    else
      puts "no skiptoken; last request"
    end
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
  puts 'number_of_records: ' + number_of_records.to_s
  if (number_of_records % 1000) == 0
   number_of_requests = (number_of_records / 1000)
  else
   number_of_requests = (number_of_records / 1000) + 1
  end
  puts 'number_of_requests: ' + number_of_requests.to_s

  results = []
  skiptoken = "" # first request without skiptoken
  number_of_requests.times {
    listings = (SparkApi.client.get("/Property", {
                                      :$top => 1000,
                                      :$filter => "ModificationTimestamp gt #{last_updated}",
                                      :$expand=>"Media,CustomFields",
                                      :$skiptoken => skiptoken
                                   }))

    swap_readable_values(listings, fields_to_lookup, metadata_xml)

    results += listings["value"]
    unless listings["@odata.nextLink"].nil? # the last page of results does not return a skiptoken
      skiptoken = CGI.parse(URI.parse(listings["@odata.nextLink"]).query)["$skiptoken"][0]
      puts "skiptoken for next request: #{skiptoken}"
    else
      puts "no skiptoken; last request"
    end
  }

  # update results
  file = File.open("updated_listings_#{now_utc}.json", "w")
  file.puts results.to_json
  file.close
  puts "Created file 'updated_listings_#{now_utc}.json'"
end
