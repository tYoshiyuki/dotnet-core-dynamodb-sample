using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DotnetCoreDynamodbSample.Services
{
    public interface IDynamoDbService
    {
        Task<List<string>> ListTablesAsync(ListTablesRequest request = default);
        Task<Dictionary<string, AttributeValue>> GetItemsAsync(GetItemRequest request);
        Task CreateTableAsync(CreateTableRequest request);
        Task DeleteTableAsync(DeleteTableRequest request);
    }

    public class DynamoDbService : IDynamoDbService
    {
        private readonly IAmazonDynamoDB _amazonDynamoDb;

        public DynamoDbService(IAmazonDynamoDB amazonDynamoDb)
        {
            _amazonDynamoDb = amazonDynamoDb;
        }

        public async Task<List<string>> ListTablesAsync(ListTablesRequest request = null)
        {
            var res = request == null ? await _amazonDynamoDb.ListTablesAsync()
                : await _amazonDynamoDb.ListTablesAsync(request);
            return res.TableNames;
        }

        public async Task<Dictionary<string, AttributeValue>> GetItemsAsync(GetItemRequest request)
        {
            var res = await _amazonDynamoDb.GetItemAsync(request);
            return res.Item;
        }

        public async Task CreateTableAsync(CreateTableRequest request)
        {
            await _amazonDynamoDb.CreateTableAsync(request);
        }

        public async Task DeleteTableAsync(DeleteTableRequest request)
        {
            await _amazonDynamoDb.DeleteTableAsync(request);
        }
    }
}
