/******************************************************/
//       THIS IS A GENERATED FILE - DO NOT EDIT       //
/******************************************************/

#include "Particle.h"
#line 1 "/Users/julianrutten/Documents/Repositories/PowerMonitor/src/PowerMonitor.ino"
// rf95_server.pde
// -*- mode: C++ -*-
// Example sketch showing how to create a simple messageing server
// with the RH_RF95 class. RH_RF95 class does not provide for addressing or
// reliability, so you should only use RH_RF95  if you do not need the higher
// level messaging abilities.
// It is designed to work with the other example rf95_client
// Tested with Anarduino MiniWirelessLoRa

#include "SPI.h"
#include "InfluxDB.h"
#include "authenticate.h"

void setup_Options();
void setup();
void loop();
void publishValuesInflux();
void publishValues();
void VoltSpike();
void ChangedPower();
float getDecimalDay();
void tryConnectWifi();
void tryConnectCloud();
void mycustomScan();
bool compareSSIDs(const char *ssid1,const char *ssid2);
#line 14 "/Users/julianrutten/Documents/Repositories/PowerMonitor/src/PowerMonitor.ino"
SYSTEM_MODE(SEMI_AUTOMATIC);

STARTUP(setup_Options());
void setup_Options()
{
  //WiFi.selectAntenna(ANT_EXTERNAL);
  System.enableFeature(FEATURE_RETAINED_MEMORY);
  // System.enableFeature(FEATURE_RESET_INFO);
  // System.disableUpdates();
  // System.enableFeature(FEATURE_WIFI_POWERSAVE_CLOCK);
}

SerialLogHandler logHandler;

InfluxDB server(IPAddress(3,88,207,237),8086,"PowerMonitor01");


const float coulomb = 0.614439;
const float mAh = 0.1707;
int pwr = D2;
int interup = D5;

double totalCoulomb = 0;
double onCoulomb = 0;
double offCoulomb = 0;
double total_mAh = 0;
double on_mAh = 0;
double off_mAh = 0;
String powerOnStampList[10];
String powerOffStampList[10];
int nOn;
int nOff;
String powerOnStamp = "";
String powerOffStamp = "";

bool updateSerial = false;
bool isPowered = false;

//////////////////////////////////////////////////////////////
//            WIFI AND TIMER SETTINGS
//////////////////////////////////////////////////////////////

retained bool hasAddedNewSSID = false;
bool wifiReady = false;
bool cloudReady = false;
bool connectingToCloud = false;

int cloudConnectionDuration = 5000; //ms --will turn off wifi and set isCloudLoop = false
int wifiConnectionDuration = 5000;  //ms --used in waitfor(WiFi.ready)
int wiFiConnectionAttemptMAX = 3;
int wiFiConnectionAttempts = 0;


/*DO NOT CHANGE*/ const char *PASS = "4Qiud29-da";
///*DO NOT CHANGE*/ const char *SSID1 = "3G uFi_C6B";

///*DO NOT CHANGE*/ const char *SSID2 = "3G uFi_9E3";
///*DO NOT CHANGE*/ const char *PASS2 = "4Qiud29-da";

///*DO NOT CHANGE*/ const char *SSID3 = "3G uFi_C83";
///*DO NOT CHANGE*/ const char *PASS3 = "4Qiud29-da";

///*DO NOT CHANGE*/ const char *SSID4 = "3G uFi_61B";
///*DO NOT CHANGE*/ const char *PASS4 = "4Qiud29-da";

///*DO NOT CHANGE*/ const char *SSID5 = "3G uFi_9E0";
///*DO NOT CHANGE*/ const char *PASS5 = "4Qiud29-da";

/*DO NOT CHANGE*/ const char *SSID1 = "3G uFi_";
/*DO NOT CHANGE*/ const char *PASS1 = "4Qiud29-da";

/*DO NOT CHANGE*/ const char *SSID2 = "Holland";
/*DO NOT CHANGE*/ const char *PASS2 = "pha813rdl470";

/*DO NOT CHANGE*/ const char *SSID3 = "NFTea";
/*DO NOT CHANGE*/ const char *PASS3 = "corimepehi";//"pha813rdl470";

bool syncedTime = false;

void setup()
{
  pinMode(pwr, INPUT_PULLDOWN);
  pinMode(interup, INPUT);
  Serial.begin(9600);
  Time.zone(+10);

  // while (!Serial.isConnected())
  //     Particle.process();
  Serial.println("Running Setup for loop");
  delay(100);
  attachInterrupt(interup, VoltSpike, FALLING);
  //attachInterrupt(pwr, ChangedPower, CHANGE);
  // Defaults after init are 434.0MHz, 13dBm, Bw = 125 kHz, Cr = 4/5, Sf = 128chips/symbol, CRC on
  Particle.connect();
  server.init();
  //tryConnectCloud();
}

void loop()
{
  if (isPowered != digitalRead(pwr))
  {
    isPowered = digitalRead(pwr);
    if (!isPowered)
    {
      updateSerial = true;
    }
    int isPowered = digitalRead(pwr);

    if (isPowered == 1)
    {
      Serial.println("now High");
      powerOnStampList[nOn] = String(getDecimalDay(), 4);
      server.add("power",1,Time.now());
      nOn++;
      //powerOnStamp += String(getDecimalDay(),4)+"_";
    }
    else
    {
      Serial.println("now Low");
      delay(10);
      powerOffStampList[nOff] = String(getDecimalDay(), 4);
      server.add("power",0,Time.now());
      nOff++;
      //powerOffStamp += String(getDecimalDay(),4)+"_";
    }
  }

  if (Time.year() != 1999)
  {
    syncedTime = true;
  }

  if (!syncedTime && Particle.connected())
  {
    Particle.syncTime();
  }

  //tryConnectCloud();
  Serial.println("Total power is \t" + String(totalCoulomb) + "C\t" + String(total_mAh) + "mAh");
  if (updateSerial)
  {
    Serial.println("Power of device switched.");
    Serial.println("Power is " + isPowered ? "On" : "off");
    Serial.println("Total power is \t" + String(totalCoulomb) + "C\t" + String(total_mAh) + "mAh");
    Serial.println("When power is on \t" + String(onCoulomb) + "C\t" + String(on_mAh) + "mAh");
    Serial.println("When power is off\t" + String(offCoulomb) + "C\t" + String(off_mAh) + "mAh");

    publishValues();
    publishValuesInflux();
    updateSerial = false;
  }
  delay(1000);
}

void publishValuesInflux()
{
  String message = "";

  if (Particle.connected())
  {
    unsigned long timestamp = Time.now();
    server.add("TotalCoulomb",totalCoulomb,timestamp);
    server.add("OnCoulomb",onCoulomb,timestamp);
    server.add("OffCoulomb",offCoulomb,timestamp);

    server.add("Total_mAh",total_mAh,timestamp);
    server.add("On_mAh",on_mAh,timestamp);
    server.add("Off_mAh",off_mAh,timestamp);

    
  if (server.sendAll("sjaak", "SystemsGarden", USERNAME, PASSWORD))
  { // call this to send points to InfluxDB
    Serial.println("InfluxDB updated");
  }
  else
  {
    Serial.println("InfluxDB NOT updated");
  }
    

  }
  //Serial.println("Sent to influxDB... ");
  //Serial.println(message);
}

void publishValues()
{
  String message = "";

  if (Particle.connected())
  {
    powerOffStamp = "";
    powerOnStamp = "";
    for (int i = 0; i < nOff; i++)
    {
      powerOffStamp.concat(powerOffStampList[i]);
      powerOffStamp.concat("_");
    }

    for (int i = 0; i < nOn; i++)
    {
      powerOnStamp.concat(powerOnStampList[i]);
      powerOnStamp.concat("_");
    }
    nOff = 0;
    nOn = 0;

    message.concat(String("{ "));
    message.concat("\"Bt\":" + String(total_mAh, 3) + ", ");
    message.concat("\"Bp\":" + String(on_mAh, 3) + ", ");
    message.concat("\"Bh\":" + String(off_mAh, 3) + ", ");
    message.concat("\"Yr\":" + String(Time.year()) + ", ");
    message.concat("\"Mo\":" + String(Time.month()) + ", ");
    message.concat("\"Da\":" + String(Time.day()) + ", ");
    message.concat("\"Hr\":" + String(Time.hour()) + ", ");
    message.concat("\"Mi\":" + String(Time.minute()) + ", ");
    message.concat("\"Sc\":" + String(Time.second()) + ", ");
    message.concat("\"Zo\":" + String(10) + ", ");

    message.concat(String("\"P1\": \""));
    message.concat(powerOffStamp + "\", ");

    message.concat(String("\"P2\": \""));
    message.concat(powerOnStamp + "\", ");

    message.concat("\"Dv\": \"Power1\" }");

    Particle.publish("Power", String(message), 60, PUBLIC); // WebHook to Google Sheets
    delay(1000);
  }
  Serial.println("Sent to the cloud... ");
  Serial.println(message);
}

void VoltSpike()
{
  total_mAh += mAh;
  totalCoulomb += coulomb;
  int isPowered = digitalRead(pwr);
  if (isPowered == 1)
  {
    on_mAh += mAh;
    onCoulomb += coulomb;
  }
  else
  {
    off_mAh += mAh;
    offCoulomb += coulomb;
  }
}

void ChangedPower()
{

/*
  Serial.println("Power changed");
  delay(10);
  if (changePower)
    changePower = false;
  else
  {
    changePower = true;
  }
    //int isPowered = digitalRead(pwr);
  if (isPowered == 1)
  {
    
    Serial.println("now High");
    delay(10);
    powerOnStampList[nOn] = String(getDecimalDay(),4);
    nOn++;
    //powerOnStamp += String(getDecimalDay(),4)+"_";
    isPowered = true;
    //  on_mAh = 0;
    //  onCoulomb = 0;
  }
  else
  {
    Serial.println("now Low");
    delay(10);
    powerOffStampList[nOff] = String(getDecimalDay(),4);
    nOff++;
    //powerOffStamp += String(getDecimalDay(),4)+"_";
    isPowered = false;
    //updateSerial = true;
    //   offCoulomb = 0;
    //   off_mAh = 0;
  }
  
    Serial.println("Saved date...");
    */
}

float getDecimalDay()
{
  int year = Time.year();
  Log.info("Current year is: " + String(year));
  long secFromEpoc = Time.local();
  Log.info("Current seconds are:: " + String(secFromEpoc));
  int additionalSeconds = 0;
  if (year > 2020)
  {
    additionalSeconds = floor((year - 2021) / 4 + 1) * 86400;
  }
  long secStartOfYear = 1546300800 + (year - 2019) * 31536000 + additionalSeconds;
  Log.info("Seconds for the start of year: " + String(secStartOfYear));
  long diffFromSoY = secFromEpoc - secStartOfYear;
  float hrs = diffFromSoY * 1.0 / 3600.0;
  Log.info("Decimal hrs are: " + String(hrs));
  return hrs;
}

void tryConnectWifi()
{
#if (PLATFORM_ID == PLATFORM_ARGON || PLATFORM_ID == PLATFORM_PHOTON_PRODUCTION)
  if (wiFiConnectionAttempts < wiFiConnectionAttemptMAX)
  {
    WiFi.on();
    WiFi.connect();
    Log.info("Connecting WiFi...");
    if (!hasAddedNewSSID)
    {
      mycustomScan();
    }
    Log.info("Connecting to Cloud..."); ///////////////////// UPDATE
    tryConnectCloud();
  }
  else
  {
    //cancelWiFi();
  }
#endif
}

void tryConnectCloud()
{
#if (PLATFORM_ID == PLATFORM_ARGON || PLATFORM_ID == PLATFORM_PHOTON_PRODUCTION)

  if (true)
  {
    if ((WiFi.ready() || WiFi.connecting())) //Mesh.ready() || Mesh.connecting())
    {
      if (WiFi.ready())
        Log.info("Wifi is ready.");

      if (WiFi.connecting())
        Log.info("Wifi is connecting.");

      if (waitFor(WiFi.ready, wifiConnectionDuration)) //waitFor(Mesh.ready,wifiConnectionDuration))//
      {
        Log.info("Wifi is ready. now to connect to the cloud. ");
        wifiReady = true;
        connectingToCloud = true;
        Particle.connect();

        if (Particle.connected())
        {
          Log.info("Connected to the cloud... ");
          cloudReady = Particle.connected();
          connectingToCloud = false;
          wiFiConnectionAttempts = 0;
          System.enableUpdates();
          Log.info("All setup, connected and ready to dance. Hazar");
        }
        else
        {
          Log.info("should be connected to cloud... ");
          Particle.connect();
        }
      }
      else
      {
        Log.info("WiFi connection timed out, will try and connect...");
        wiFiConnectionAttempts++;
        tryConnectWifi();
      }
      if (WiFi.connecting())
      {
        Log.info("We are still connecting to the cloud. Will try connect again.");
        wiFiConnectionAttempts++;
        tryConnectWifi();
      }
    }
    else
    {
      Log.info("Is a cloud loop but Wifi is not enabled, will now connect.");

      if (wiFiConnectionAttempts > 1)
        Log.info("Not connected to the cloud. will try again...");

      wiFiConnectionAttempts++;
      tryConnectWifi();
    }
  }
  if (Particle.connected() == false)
  {
    cloudReady = Particle.connected();
    Log.info("Connecting to the cloud... ");
    Particle.connect();
    Log.info("should be connected to cloud... ");
  }
#endif
}

void mycustomScan()
{
  WiFiAccessPoint aps[20];
  WiFiAccessPoint Myap[5];
  //int foundMy = WiFi.getCredentials(Myap, 5); //-1;//
  int found = WiFi.scan(aps, 20);             //0;//
  for (int i = 0; i < found; i++)
  {
    WiFiAccessPoint &ap = aps[i];

/*
    if (ap.ssid[0] == '3' && ap.ssid[1] == 'G')
    {
      bool haveGot = false;

      for (int j = 0; j < foundMy; j++)
      {
        if (Myap[j].ssid == ap.ssid)
        {
          haveGot = true;
          break;
        }
      }
      if (!haveGot)
      {
        Serial.print("SSID: ");
        Serial.println(ap.ssid);
        WiFi.setCredentials(ap.ssid, PASS); //
        hasAddedNewSSID = true;
      }
      else
      {
        Serial.print("I already know SSID: ");
        Serial.println(ap.ssid);
      }
    }
    */
    if (compareSSIDs(ap.ssid,SSID1))//(ap.ssid[0] == 'H' && ap.ssid[1] == 'o' && ap.ssid[2] == 'l' && ap.ssid[3] == 'l' && ap.ssid[4] == 'a' && ap.ssid[5] == 'n' && ap.ssid[6] == 'd')
    {

      Serial.print("SSID: ");
      Serial.println(ap.ssid);
      WiFi.setCredentials(ap.ssid, PASS1); //
      hasAddedNewSSID = true;
    }
    else if (compareSSIDs(ap.ssid,SSID2))//(ap.ssid[0] == 'H' && ap.ssid[1] == 'o' && ap.ssid[2] == 'l' && ap.ssid[3] == 'l' && ap.ssid[4] == 'a' && ap.ssid[5] == 'n' && ap.ssid[6] == 'd')
    {

      Serial.print("SSID: ");
      Serial.println(ap.ssid);
      WiFi.setCredentials(ap.ssid, PASS2); //
      hasAddedNewSSID = true;
    }else if (compareSSIDs(ap.ssid,SSID3))
    {

      Serial.print("SSID: ");
      Serial.println(ap.ssid);
      WiFi.setCredentials(ap.ssid, PASS3); //
      hasAddedNewSSID = true;
    }
    else
    {

      Serial.print("Unknown SSID: ");
      Serial.println(ap.ssid);
    }
  }
}

bool compareSSIDs(const char *ssid1,const char *ssid2){
  String test1 = String(ssid1);
  String test2 = String(ssid2);
  int count = 0;
  if (test1.length()>test2.length())
    count = test2.length();
  else
    count = test1.length();
  
  bool hasName = true;
  for (int i = 0; i < count; i++)
  {
    if(ssid1[i]!=ssid2[i]){
      hasName = false;
      break;
    }
  }
  return hasName;
}