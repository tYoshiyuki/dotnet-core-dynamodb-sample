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

            await service.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>()
                {
                      { "Id", new AttributeValue { N = "201" }},
                      { "Title", new AttributeValue { S = "Book 201 Title" }},
                      { "ISBN", new AttributeValue { S = "11-11-11-11" }},
                      { "Price", new AttributeValue { N = "100" }},
                      { "Authors", new AttributeValue { SS = new List<string>{"Author1", "Author2"} }}
                }
            });

            // 確認用の関数
            async void outItem()
            {
                var item = await service.GetItemsAsync(new GetItemRequest
                {
                    TableName = tableName,
                    Key = new Dictionary<string, AttributeValue>() { { "Id", new AttributeValue { N = "201" } } }
                });
                item.TryGetValue("Id", out AttributeValue id);
                Console.WriteLine("---- Output Item Info ----");
                Console.WriteLine($"Id:{id?.N.ToString() ?? "No Data"}");

                item.TryGetValue("Price", out AttributeValue price);
                Console.WriteLine($"Price:{price?.N.ToString() ?? "No Data"}");
            }
            outItem();

            await service.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue>() { { "Id", new AttributeValue { N = "201" } } },
                ExpressionAttributeNames = new Dictionary<string, string>()
                {
                    {"#A", "Authors"},
                    {"#P", "Price"},
                    {"#NA", "NewAttribute"},
                    {"#I", "ISBN"}
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    {":auth", new AttributeValue { SS = {"Author YY","Author ZZ"}}},
                    {":p", new AttributeValue {N = "1"}},
                    {":newattr", new AttributeValue {S = "someValue"}},
                },
                // This expression does the following:
                // 1) Adds two new authors to the list
                // 2) Reduces the price
                // 3) Adds a new attribute to the item
                // 4) Removes the ISBN attribute from the item
                UpdateExpression = "ADD #A :auth SET #P = #P - :p, #NA = :newattr REMOVE #I"
            });
            outItem();

            await service.DeleteItemAsync(new DeleteItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue>() { { "Id", new AttributeValue { N = "201" } } }
            });
            outItem();

            await service.DeleteTableAsync(new DeleteTableRequest { TableName = tableName });
        }
    }
}
