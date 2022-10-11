using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Azure;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using System.IO;
using Azure.Core.Pipeline;
using System.Net.Http;

namespace ADTCommandTools
{
    public class Program
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private static string adtInstanceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        
        public static async Task Main(string[] args)
        {
            var credentials = new DefaultAzureCredential();
            var client = new DigitalTwinsClient(new Uri(adtInstanceUrl), 
                credentials, 
                new DigitalTwinsClientOptions { Transport = new HttpClientTransport(httpClient) });
            Console.WriteLine($"Service client created – ready to go");

            string twinId = "serra01";
            //Print twin
            Console.WriteLine("--- Printing twin details:");
            await CustomMethod_FetchAndPrintTwinAsync(twinId, client);
            Console.WriteLine("--------");

            //Update twin data
            var updateTwinData = new JsonPatchDocument();
            updateTwinData.AppendAdd("/Moisture", 30.0);
            // <UpdateTwinCall>
            await client.UpdateDigitalTwinAsync(twinId, updateTwinData);
            // </UpdateTwinCall>
            Console.WriteLine("Twin properties updated");
            Console.WriteLine();

            //Print twin again
            Console.WriteLine("--- Printing twin details (after update):");
            await CustomMethod_FetchAndPrintTwinAsync(twinId, client);
            Console.WriteLine("--------");
            Console.WriteLine();
        }

        private static async Task<BasicDigitalTwin> CustomMethod_FetchAndPrintTwinAsync(string twinId, DigitalTwinsClient client)
        {
            Response<BasicDigitalTwin> twinResponse = await client.GetDigitalTwinAsync<BasicDigitalTwin>(twinId);
            var twin = twinResponse.Value;

            Console.WriteLine($"Model id: {twin.Metadata.ModelId}");
            foreach (string prop in twin.Contents.Keys)
            {
                if (twin.Contents.TryGetValue(prop, out object value))
                    Console.WriteLine($"Property '{prop}': {value}");
            }

            return twin;
        }
    }
}