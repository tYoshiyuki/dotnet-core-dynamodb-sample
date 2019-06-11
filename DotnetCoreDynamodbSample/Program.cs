using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DotnetCoreDynamodbSample.Config;
using DotnetCoreDynamodbSample.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DotnetCoreDynamodbSample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // 設定ファイルの読み込み
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            var configuration = builder.Build();

            // サービスの設定
            var serviceCollection = new ServiceCollection();

            var dynamoDbConfig = new DynamoDbConfig();
            configuration.Bind("DynamoDb", dynamoDbConfig);

            if (dynamoDbConfig.LocalMode)
            {
                serviceCollection.AddSingleton<IAmazonDynamoDB>(sp =>
                {
                    var clientConfig = new AmazonDynamoDBConfig { ServiceURL = dynamoDbConfig.LocalServiceUrl };
                    return new AmazonDynamoDBClient(clientConfig);
                });
            }
            else
            {
                serviceCollection.AddAWSService<IAmazonDynamoDB>();
            }

            serviceCollection.AddTransient<IDynamoDbService, DynamoDbService>();
            var service = serviceCollection.BuildServiceProvider().GetService<IDynamoDbService>();

            // サンプル実装
            var tables = await service.ListTablesAsync();
            Console.WriteLine(string.Join(",", tables));

            var tableName = "DynamoSample";
            if (!tables.Any(_ => _ == tableName))
            {
                await service.CreateTableAsync(new CreateTableRequest
                {
                    TableName = "DynamoSample",
                    AttributeDefinitions = new List<AttributeDefinition>
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Id",
                        AttributeType = "N"
                    }
                },
                    KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement
                    {
                        AttributeName = "Id",
                        KeyType = "HASH"  //Partition key
                    }
                },
                    ProvisionedThroughput = new ProvisionedThroughput
                    {
                        ReadCapacityUnits = 2,
                        WriteCapacityUnits = 2
                    }
                });
            }

            // TODO insert, delete item の実装

            await service.DeleteTableAsync(new DeleteTableRequest { TableName = tableName });
        }
    }
}
