
/*
 InfluxDB library
 Copyright 2018 Richard Lyon
 Licensed under the GNU General Public license
 */

#ifndef _INFLUXDB_
#define _INFLUXDB_
#endif
#ifndef VERSION
#define VERSION 0.1.0
#endif
#ifndef IP
#define IP IPAddress(3,88,207,237)
#endif
#ifndef PORT
#define PORT 8086
#endif
#ifndef MAX_VALUES
#define MAX_VALUES 500
#endif
#ifndef DATABASE
#define DATABASE "sensordata"
#endif
#ifndef DEVICENAME
#define DEVICENAME "particle"
#endif

#include "Particle.h"
#include "HttpClient.h"

struct Value {
    char  *idName;
    float idValue;
    unsigned long timestamp_val;
};

class InfluxDB
{
public:
  InfluxDB(IPAddress serverIP, int serverPort, String devName);
  void init();
  void add(char *variable_id, double value);
  void add(char *variable_id, double value, unsigned long timestamp);
  bool sendAll(char* organisation, char* bucket, char* tokenName, char* tokenKey);
  void setDatabase(char* organisation, char* bucket);
  void setToken(char* tokenName, char* tokenKey);
  void addTag(String tag,String Value);
  String setDeviceName(String databaseName);
  void setDebug(bool debug);
private:
  char* _tokenName;
  char* _tokenKey;
  char* _organisation;
  char* _bucket;
  String _deviceID;
  String _deviceName;
  String _tags;
  //String _databaseName;
  Value* pvalue;
  uint8_t _currentValue;
  HttpClient http;
  http_request_t request;
  http_response_t response;
  bool _debug = false;
  String makeFieldSet(Value* pvalue);
  void printDebug(http_request_t &request, http_response_t &response);
};
