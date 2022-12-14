using Azure;
using Azure.DigitalTwins.Core;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace SampleFunctionsApp
{
    internal static class AdtUtilities
    {
        public static async Task<string> FindParentAsync(DigitalTwinsClient client, string child, string relname, ILogger log)
        {
            // Find parent using incoming relationships
            try
            {
                AsyncPageable<IncomingRelationship> rels = client.GetIncomingRelationshipsAsync(child);
                await foreach (IncomingRelationship ie in rels)
                {
                    log.LogInformation($"Incoming Relationship found. The name is \"{ie.RelationshipName}\"");
                    if (ie.RelationshipName == relname)
                        return (ie.SourceId);
                }
            }
            catch (RequestFailedException exc)
            {
                log.LogError($"*** Error in retrieving parent:{exc.Status}:{exc.Message}");
            }
            log.LogInformation($"No incoming Relationship found.");
            return null;
        }

        public static async Task<string> FindParentByQueryAsync(DigitalTwinsClient client, string childId, ILogger log)
        {
            string query = "SELECT Parent " +
                           "FROM digitaltwins Parent " +
                           "JOIN Child RELATED Parent.contains " +
                           $"WHERE Child.$dtId = '{childId}'";
            log.LogInformation($"Query: {query}");

            try
            {
                AsyncPageable<BasicDigitalTwin> twins = client.QueryAsync<BasicDigitalTwin>(query);
                await foreach (BasicDigitalTwin twin in twins)
                {
                    return twin.Id;
                }
                log.LogWarning($"*** No parent found");
            }
            catch (RequestFailedException exc)
            {
                log.LogError($"*** Error in retrieving parent:{exc.Status}/{exc.Message}");
            }
            return null;
        }

        public static async Task UpdateTwinPropertyAsync<T>(DigitalTwinsClient client, string twinId, string propertyPath, T value, ILogger log)
        {
            // If the twin does not exist, this will log an error
            try
            {
                var updateTwinData = new JsonPatchDocument();
                updateTwinData.AppendReplace(propertyPath, value);

                log.LogInformation($"UpdateTwinPropertyAsync ({twinId}) sending {updateTwinData}");
                await client.UpdateDigitalTwinAsync(twinId, updateTwinData);
            }
            catch (RequestFailedException exc)
            {
                log.LogError($"*** Error:{exc.Status}/{exc.Message}");
            }
        }
    }
}
