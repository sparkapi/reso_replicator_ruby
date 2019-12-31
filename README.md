# Overview
This script is an example of how to replicate data, then periodically retrieve newly modified data, to be used to keep your local data in sync with data available via the [RESO Web API](http://sparkplatform.com/docs/reso/overview). It is meant to be scheduled to run at a regular interval (e.g. as a cron job, etc.).
* The first time it is run, it retrieves the data for all listings available to your access token via the `/listings` resource, creating a local `listings.json` file
* Subsequent runs download `listings` that have been modified since the last time the script checked for new data and save them to time-stamped `updated_listing` files.

This script does _not_ merge newly modified data with the originally replicated data. How that is done will depend on how you are storing the data.

Whenever possible, this script also substitutes human-friendly values for encoded strings in the API responses (necessary when certain keys and values contain spaces and special characters), in:
* Field values
* CustomField names
* Media labels

To properly use this script, you should have an API key with a "replication" role, which allows larger requests than a normal API key. Without it, you are limited to pulling a maximum of 25 records per API call, and you must change the `config.endpoint` to `https://sparkapi.com`.

This script uses the [Spark API Ruby client](https://github.com/sparkapi/spark_api).

# Improvements
- [x] Translate custom field names
- [ ] Log full API requests to console for debugging purposes
- [x] Allow users to specify how many records to pull in each request, and update the number of calls needed accordingly
- [ ] Add support for additional resources (`/Office`, `/Member`)
- [ ] Decrease memory required by writing API response data to file for each call rather than once at the end
