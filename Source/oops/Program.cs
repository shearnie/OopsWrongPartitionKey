using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System.Linq;
using Newtonsoft.Json;

namespace oops
{
    class Program
    {
        static void Main(string[] args)
        {
            var source = GetClient("Source");
            var target = GetClient("Target");
            
            var rs = GetNext(source.Client, source.CollectionUri, null, 20);
            while (rs.Records.Any())
            {
                Console.WriteLine(rs.ContinuationToken + " " + rs.Records.Count);
                if (string.IsNullOrEmpty(rs.ContinuationToken))
                    break;

                rs = GetNext(source.Client, source.CollectionUri, rs.ContinuationToken, 20);
            }

            Console.ReadLine();
        }

        public class PartialQueryResult<T>
        {
            public List<T> Records;
            public string ContinuationToken;
        }

        static PartialQueryResult<dynamic> GetNext(IDocumentClient client, Uri collectionUri, string continuationToken, int numberOfRecords)
        {
            var queryable = client.CreateDocumentQuery<dynamic>(
                    collectionUri,
                    "SELECT * FROM c",
                    new FeedOptions
                    {
                        EnableCrossPartitionQuery = true,
                        MaxItemCount = numberOfRecords,
                        RequestContinuation = continuationToken,
                    })
                .AsDocumentQuery();

            return NextPage<dynamic>(queryable);
        }

        static PartialQueryResult<T> NextPage<T>(IDocumentQuery<T> queryable)
        {
            var feedResponse = queryable.ExecuteNextAsync().Result;
            var continuationToken = feedResponse.ResponseContinuation;

            return new PartialQueryResult<T>()
            {
                Records = feedResponse
                    .Select(record => JsonConvert.DeserializeObject<T>(record.ToString()))
                    .Cast<T>()
                    .ToList(),
                ContinuationToken = continuationToken
            };
        }

        static (IDocumentClient Client, Uri CollectionUri) GetClient(string description)
        {
            Console.Write($"{description} endpoint: ");
            var endpoint = Console.ReadLine();

            Console.Write($"{description} key: ");
            var key = Console.ReadLine();

            Console.Write($"{description} database: ");
            var db = Console.ReadLine();

            Console.Write($"{description} collectionid: ");
            var cid = Console.ReadLine();

            var client = new DocumentClient(new Uri(endpoint), key);
            var collectionUri = UriFactory.CreateDocumentCollectionUri(db, cid);

            return (client, collectionUri);
        }

        static (IDocumentClient Client, Uri CollectionUri) GetClient(string endpoint, string key, string db, string cid)
        {
            var client = new DocumentClient(new Uri(endpoint), key);
            var collectionUri = UriFactory.CreateDocumentCollectionUri(db, cid);

            return (client, collectionUri);
        }
    }
}
