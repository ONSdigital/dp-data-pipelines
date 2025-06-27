# Data flow and component architecture

```mermaid
graph TD
    subgraph "Input Sources"
        S3Input[S3 Bucket: Zip File]
        Secrets[AWS Secrets Manager: Configuration]
        DatasetApi[Dataset APMetadataLoader: Existing Metadata]
    end
    
    subgraph "Lambda Functions"
        TriggerLambda[lambda_triggers_etl]
        ETLLambda[lambda_job_etl]
    end
    
    subgraph "Core Processing Classes"
        ETLProcessor[ETLProcessor]
        S3Obj[S3Object]
        ZipFile[ProcessedZipFile]
        MetadataLoader[MetadataLoader]
        LocalDirStore[LocalDirectoryStore]
    end
    
    subgraph "Data Models"
        Manifest[Manifest]
        Metadata[Metadata]
        Distribution[Distribution]
        Dataset[Dataset]
        DatasetStatus[DatasetStatus]
        DatasetEvent[DatasetEvent]
    end
    
    subgraph "Service Clients"
        DatasetAPMetadataLoaderService[DatasetAPMetadataLoaderService]
        UploadServiceClient[UploadServiceClient]
        SesClient[SesClient - Email]
        SlackMessenger[SlackMessenger - Notifications]
    end
    
    subgraph "Database Collections"
        DatasetsCollection[DatasetsCollection]
        DatasetStatusesCollection[DatasetStatusesCollection]
        DocumentDB[DocumentDB/MongoDB]
    end
    
    subgraph "Output Destinations"
        DatasetAPI[Dataset API: Metadata]
        UploadService[Upload Service: Data Files]
        Emails[Email: Notifications]
        Slack[Slack: Status Updates]
        S3Output[S3: Processed/Failed Files]
    end
    
    %% Flow connections
    S3Input --> TriggerLambda
    TriggerLambda --> ETLLambda
    Secrets --> ETLProcessor
    ETLLambda --> ETLProcessor
    
    ETLProcessor --> S3Obj
    S3Obj --> ZipFile
    ETLProcessor --> MetadataLoader
    ZipFile --> LocalDirStore
    
    LocalDirStore --> Manifest
    MetadataLoader --> Metadata
    Metadata --> Distribution
    
    ETLProcessor--> Dataset
    Dataset --> DatasetStatus
    DatasetStatus --> DatasetEvent
    
    ETLProcessor--> DatasetAPMetadataLoaderService
    ETLProcessor--> UploadServiceClient
    ETLProcessor--> SesClient
    ETLProcessor--> SlackMessenger
    
    ETLProcessor--> DatasetsCollection
    ETLProcessor--> DatasetStatusesCollection
    DatasetsCollection --> DocumentDB
    DatasetStatusesCollection --> DocumentDB
    
    DatasetAPMetadataLoaderService --> DatasetAPI
    UploadServiceClient --> UploadService
    SesClient --> Emails
    SlackMessenger --> Slack
    ETLProcessor--> S3Output
    
    %% Data flow for success path
    DatasetApi --> MetadataLoader
    
    %% Styling
    classDef input fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    classDef lambda fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    classDef core fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef model fill:#e8f5e8,stroke:#388e3c,stroke-width:2px
    classDef service fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    classDef db fill:#e0f2f1,stroke:#00695c,stroke-width:2px
    classDef output fill:#fff8e1,stroke:#f9a825,stroke-width:2px
    
    class S3Input,Secrets,DatasetApi input
    class TriggerLambda,ETLLambda lambda
    class ETLProcessor,S3Obj,ZipFile,MetadataLoader,LocalDirStore core
    class Manifest,Metadata,Distribution,Dataset,DatasetStatus,DatasetEvent model
    class DatasetAPMetadataLoaderService,UploadServiceClient,SesClient,SlackMessenger service
    class DatasetsCollection,DatasetStatusesCollection,DocumentDB db
    class DatasetAPI,UploadService,Emails,Slack,S3Output output
```