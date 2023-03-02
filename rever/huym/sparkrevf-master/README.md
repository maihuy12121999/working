# RSparkFlow - Rever Spark Flow

Flow:  `Reader -> Flow -> Writer`

## Install

- Maven

```xml

<dependency>
    <groupId>rever.etl</groupId>
    <artifactId>rsparkflow</artifactId>
    <version>1.5.5</version>
</dependency>
```

## 1. Project structure.

---

Create 3 following folders under your package.

```
your_package
└───config
│   │   ...
│   │   ...
└───reader
│   │   ...
│   │   ...
└───writer
│   │   ...
│   │   ...
└───your_flow.scala
```

Where:

- `config`: Define your custom Configuration class
- `reader`: Define your Data reader
- `writer`: your Data Writer

## 2. Source Readers

--- 
Create your reader implement from `rever.etl.rsparkflow.api.SourceReader`

## 3. Sink Writers

---
Create your writer implement from `rever.etl.rsparkflow.api.SinkWriter`

## 4. Examples

See more in the `test` folder.

- Path: `src/test/scala/rever/etl/example`

## 5. Limitations

---

1. It still starts the flow and invoke `SourceReader` without any `SinkWriter` was defined.

## 6. Others

- If you are working with AWS S3, add this to your dependency:

```yml
 <dependency>
 <groupId>org.apache.hadoop</groupId>
 <artifactId>hadoop-aws</artifactId>
 <version>3.1.2</version>
 </dependency>
 <dependency>
 <groupId>software.amazon.awssdk</groupId>
 <artifactId>s3</artifactId>
 <version>2.16.63</version>
 </dependency>
```

# User Define Functions

1. User Agent Parser

- Input:
    - userAgent: String
- Output:
    - `browser`: String
    - `browser_version`: String
    - `os`: String
    - `os_version`: String
    - `device`: String
    - `platform`: String



