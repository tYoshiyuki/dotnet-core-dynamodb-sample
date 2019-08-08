using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
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

            serviceCollection.AddDefaultAWSOptions(configuration.GetAWSOptions());
            serviceCollection.AddAWSService<IAmazonDynamoDB>();
            serviceCollection.AddTransient<IDynamoDBContext, DynamoDBContext>();
            serviceCollection.AddTransient<IDynamoDbService, DynamoDbService>();
            var service = serviceCollection.BuildServiceProvider().GetService<IDynamoDbService>();

            // サンプル実装
            var tables = await service.GetTableNameListAsync();
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
                UpdateExpression = "ADD #A :auth SET #P = #P - :p, #NA = :newattr REMOVE #I"
            });
            outItem();

            var entity = new SampleModel
            {
                Id = 301,
                Title = "Book 301 Title",
                ISBN = "11-11-11-12",
                Price = 300,
                Authors = new List<string> { "Author3", "Author4" }
            };

            await service.PutEntityAsync(entity);
            var result = await service.GetEntityAsync<SampleModel>(301);

            result.Price = 400;
            await service.PutEntityAsync(result);

            var condition = new List<ScanCondition> { new ScanCondition("Price", ScanOperator.Equal, 400) };
            var results = await service.GetEntityListAsync<SampleModel>(condition);

            await service.DeleteEntityAsync(results.FirstOrDefault());

            await service.DeleteItemAsync(new DeleteItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue>() { { "Id", new AttributeValue { N = "201" } } }
            });
            outItem();

            await service.DeleteTableAsync(new DeleteTableRequest { TableName = tableName });
        }
    }

    [DynamoDBTable("DynamoSample")]
    public class SampleModel
    {
        public int Id { get; set; }

        public string Title { get; set; }

        public string ISBN { get; set; }

        public int Price { get; set; }

        public List<string> Authors { get; set; }
    }
}
