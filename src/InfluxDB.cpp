/* InfluxDB library
   Copyright 2018 Richard Lyon
   Licensed under the GNU General Public license
 */

#include "HttpClient.h"
#include "InfluxDB.h"

InfluxDB::InfluxDB(IPAddress serverIP, int serverPort, String devName)
{
  _deviceID = System.deviceID();
  _deviceName = String(devName);
  //_databaseName = "sensordata";
  _currentValue = 0;
  request.port = serverIP; // influxdb port
  request.ip = serverPort;     // DigitalOcean
  request.path = "/ping";
  //request.path = String::format("/api/v2/write?org=%s&bucket=%s&precision=s", _organisation, _organisation);
  pvalue = (Value *)malloc(MAX_VALUES * sizeof(Value));
}

void InfluxDB::add(char *variable_id, double value)
{
  return add(variable_id, value, NULL);
}

void InfluxDB::add(char *variable_id, double value, unsigned long timestamp)
{
  (pvalue + _currentValue)->idName = variable_id;
  (pvalue + _currentValue)->idValue = value;

  if (timestamp)
  {
    // process batch of values
    (pvalue + _currentValue)->timestamp_val = timestamp;
  }
  else
  {
    // process single value
    (pvalue + _currentValue)->timestamp_val = Time.now();
  }

  _currentValue++;
  if (_currentValue > MAX_VALUES)
  {
    Serial.println("You are sending more than the maximum of consecutive variables");
    _currentValue = MAX_VALUES;
  }
}

void InfluxDB::init(){

  request.path = "/ping";
  http.get(request, response);
  printDebug(request, response);
}

void InfluxDB::addTag(String tag,String Value){
    _tags.concat(String::format(",%s=%s",tag.c_str(),Value.c_str()));
}

bool InfluxDB::sendAll(char *organisation, char *bucket, char *tokenName, char *tokenKey)
{

  setToken(tokenName, tokenKey);
  setDatabase(organisation, bucket);

  unsigned long lastTimestamp, currentTimestamp;
  String idMeasurement = _deviceName;                                // e.g. particle
  //addTag("deviceID",_deviceID);
  String tag_set = String::format("deviceID=%s", _deviceID.c_str()); // e.g. deviceID=54395594308
  tag_set.concat(_tags);
  _tags = "";
  for (int i = 0; i < _currentValue; i++)
  {
    String field_set = makeFieldSet(pvalue + i); // e.g. temperature=21.3
    currentTimestamp = (pvalue + i)->timestamp_val;
    while ((i + 1) < _currentValue && currentTimestamp == (pvalue + i + 1)->timestamp_val)
    { // batch
      i++;
      field_set.concat(",");
      field_set.concat(makeFieldSet(pvalue + i));
      currentTimestamp = (pvalue + i)->timestamp_val;
    }

    // e.g. particle,deviceID=54395594308 temperature=21.3,humidity=34.5 435234783725
    String requestString = String::format("%s,%s %s %d", idMeasurement.c_str(), tag_set.c_str(), field_set.c_str(), currentTimestamp);

    Particle.publish("DEBUG", requestString); // WebHook to Google Sheets
    delay(1000);
    String tokenInfo = String::format("Token %s", _tokenKey);
    http_header_t headers[] = {
        {"Authorization", tokenInfo.c_str()},
        {"Content-Type", "text/plain; charset=utf-8"},
        {"Accept", "application/json"},
        {NULL, NULL} // NOTE: Always terminate headers will NULL
    };
    // send it
    request.path = String::format("/api/v2/write?org=%s&bucket=%s&precision=s", _organisation, _bucket);
    request.body = requestString;
    http.post(request, response, headers);
  }
  _currentValue = 0;
  if (response.status == 204)
  {
    if (_debug)
      printDebug(request, response);
    return true;
  }
  else
  {
    if (_debug)
    {
      printDebug(request, response);
      Particle.publish("ERROR", response.body, PRIVATE);
    }
    return false;
  }
}

String InfluxDB::makeFieldSet(Value *pvalue)
{
  return String::format("%s=%.1f", (pvalue)->idName, (pvalue)->idValue); // e.g. temperature=21.3
}

void InfluxDB::printDebug(http_request_t &request, http_response_t &response)
{
  Serial.println(request.path);
  Serial.println(request.body);
  Serial.print("HTTP Response: ");
  Serial.println(response.status);
  Serial.println(response.body);
}

String InfluxDB::setDeviceName(String deviceName)
{
  _deviceName = deviceName;
  return deviceName;
}

void InfluxDB::setDatabase(char *organisation, char *bucket)
{
  // TODO: add check that database exists or create
  _organisation = organisation;
  _bucket = bucket;
}

void InfluxDB::setToken(char *tokenName, char *tokenKey)
{
  _tokenName = tokenName;
  _tokenKey = tokenKey;
}

void InfluxDB::setDebug(bool debug)
{
  _debug = debug;
}
