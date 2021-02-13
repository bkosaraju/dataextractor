# DataExtractor

Data extractor is spark based framework to read data from RDBMS(over JDBC) and Salesforce systems

Application Properties
----------------------
Appliaction properties to read data from source system

```properties
appName=IngestionLoader
readerFormat=
targetURI=
#DO NOT Pass target key in case of sourceTableList option is set as everything will be loaded to same URI in target.
targetKey=
sourceDatabase=
sourceTable=
#sourceTableList=table1,table2,table3
sqlFile=
readerOptions=
srcFormats=
srcToTgtColMap=

#Target Options
writeMode=
writeFormat=

#Incase of JDBC/RDBMS Writer
targetTable=
targetDatabase=
#Encrytion Options
encryptedConfig=
encryptedConfigKey=

#RDBMS specifc
rdbmsUser=
rdbmsPassword=
rdbmsUrl=

#Salesforce Specfic
salesforceUser=
salesforcePassword=
salesforceUrl=
salesforceObjects=

#extractFilter=last_updatedate>#prevjobExecutionTimestamp#
extractFilter=

#OtherOptions
extractionType=oracleBulkExtract #(adds rownum in column list so that partition extraction can be achieved)

```


Where can I get the latest release?
-----------------------------------
You can get source from [SCM](https://github.com/bkosaraju/dataextractor).

Alternatively you can pull binaries from the central Maven repositories(yet to publish):
For mvn:
```xml
<dependency>
  <groupId>io.github.bkosaraju</groupId>
  <artifactId>dataextractor_#ScalaVariant#</artifactId>
  <version>#Version#</version>
</dependency>
 
<!--Fat/ Assembly Jar-->
<dependency>
  <groupId>io.github.bkosaraju</groupId>
  <artifactId>dataextractor_#ScalaVariant#</artifactId>
  <version>#verion#</version>
  <classifier>all</classifier>
</dependency>

```
for Gradle:

```groovy
    api group: "io.github.bkosaraju", name: "dataextractor_$scalaVariant", version: "$Version"
```

## Build Instructions

```bash
./gradlew clean build

#Artifacts can be found in build/lib directory 

#Linux/Windows binaries can be found at build/distribution directory 
```

## Scala [Docs](https://bkosaraju.github.io/pipeline)

## Contributing
Please feel free to raise a pull request in case if you feel like something can be updated or contributed

## License
[Apache](http://www.apache.org/licenses/LICENSE-2.0.txt)
