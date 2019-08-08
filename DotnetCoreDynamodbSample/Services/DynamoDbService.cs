using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DotnetCoreDynamodbSample.Services
{
    public interface IDynamoDbService
    {
        Task<List<string>> GetTableNameListAsync(ListTablesRequest request = default);
        Task<Dictionary<string, AttributeValue>> GetItemsAsync(GetItemRequest request);
        Task CreateTableAsync(CreateTableRequest request);
        Task DeleteTableAsync(DeleteTableRequest request);
        Task PutItemAsync(PutItemRequest request);
        Task UpdateItemAsync(UpdateItemRequest request);
        Task DeleteItemAsync(DeleteItemRequest request);
        Task<T> GetEntityAsync<T>(int id);
        Task<IEnumerable<T>> GetEntityListAsync<T>(IEnumerable<ScanCondition> conditions);
        Task PutEntityAsync<T>(T input);
        Task DeleteEntityAsync<T>(T input);
    }

    public class DynamoDbService : IDynamoDbService
    {
        private readonly IAmazonDynamoDB _client;
        private readonly IDynamoDBContext _context;

        public DynamoDbService(IAmazonDynamoDB client, IDynamoDBContext context)
        {
            _client = client;
            _context = context;
        }

        /// <summary>
        /// テーブル名のリストを取得します。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public async Task<List<string>> GetTableNameListAsync(ListTablesRequest request = null)
        {
            var res = request == null ? await _client.ListTablesAsync()
                : await _client.ListTablesAsync(request);
            return res.TableNames;
        }

        /// <summary>
        /// 値を取得します。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public async Task<Dictionary<string, AttributeValue>> GetItemsAsync(GetItemRequest request)
        {
            var res = await _client.GetItemAsync(request);
            return res.Item;
        }

        /// <summary>
        /// テーブルを作成します。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public async Task CreateTableAsync(CreateTableRequest request)
        {
            await _client.CreateTableAsync(request);
        }

        /// <summary>
        /// テーブルを削除します。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public async Task DeleteTableAsync(DeleteTableRequest request)
        {
            await _client.DeleteTableAsync(request);
        }

        /// <summary>
        /// 値を保存します。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public async Task PutItemAsync(PutItemRequest request)
        {
            await _client.PutItemAsync(request);
        }

        /// <summary>
        /// 値を更新します。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public async Task UpdateItemAsync(UpdateItemRequest request)
        {
            await _client.UpdateItemAsync(request);
        }

        /// <summary>
        /// 値を削除します。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public async Task DeleteItemAsync(DeleteItemRequest request)
        {
            await _client.DeleteItemAsync(request);
        }

        /// <summary>
        /// エンティティを取得します。
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<T> GetEntityAsync<T>(int id)
        {
            return await _context.LoadAsync<T>(id);
        }

        /// <summary>
        /// エンティティのリストを取得します。
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="conditions"></param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> GetEntityListAsync<T>(IEnumerable<ScanCondition> conditions)
        {
            return await _context.ScanAsync<T>(conditions).GetRemainingAsync();
        }

        /// <summary>
        /// エンティティを保存します。
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public async Task PutEntityAsync<T>(T input)
        {
            await _context.SaveAsync(input);
        }

        /// <summary>
        /// エンティティを削除します。
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public async Task DeleteEntityAsync<T>(T input)
        {
            await _context.DeleteAsync(input);
        }
    }
}
