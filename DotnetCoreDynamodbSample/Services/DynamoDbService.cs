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
        Task CreateTableAsync(CreateTableRequest request);
        Task DeleteTableAsync(string tableName);
        Task<T> GetEntityAsync<T>(int id);
        Task<IEnumerable<T>> GetEntityListAsync<T>(IEnumerable<ScanCondition> conditions);
        Task PutEntityAsync<T>(T input, bool isSkipVersionCheck = false);
        Task DeleteEntityAsync<T>(T input, bool isSkipVersionCheck = false);
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
        /// <param name="tableName"></param>
        /// <returns></returns>
        public async Task DeleteTableAsync(string tableName)
        {
            await _client.DeleteTableAsync(new DeleteTableRequest { TableName = tableName });
        }

        /// <summary>
        /// エンティティを取得します。
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<T> GetEntityAsync<T>(int id)
        {
            return await _context.LoadAsync<T>(id, new DynamoDBOperationConfig { ConsistentRead = true });
        }

        /// <summary>
        /// エンティティのリストを取得します。
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="conditions"></param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> GetEntityListAsync<T>(IEnumerable<ScanCondition> conditions)
        {
            return await _context.ScanAsync<T>(conditions, new DynamoDBOperationConfig { ConsistentRead = true }).GetRemainingAsync();
        }

        /// <summary>
        /// エンティティを保存します。
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public async Task PutEntityAsync<T>(T input, bool isSkipVersionCheck = false)
        {
            await _context.SaveAsync(input, new DynamoDBOperationConfig { SkipVersionCheck = isSkipVersionCheck });
        }

        /// <summary>
        /// エンティティを削除します。
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public async Task DeleteEntityAsync<T>(T input, bool isSkipVersionCheck = false)
        {
            await _context.DeleteAsync(input, new DynamoDBOperationConfig { SkipVersionCheck = isSkipVersionCheck });
        }
    }
}
