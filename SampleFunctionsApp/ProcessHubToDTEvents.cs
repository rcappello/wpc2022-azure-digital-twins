using Azure;
using Azure.Core.Pipeline;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace SampleFunctionsApp
{
    // This class processes telemetry events from IoT Hub, reads temperature of a device
    // and sets the "Temperature" property of the device with the value of the telemetry.
    public class ProcessHubToDTEvents
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private static string adtServiceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");

        [FunctionName("ProcessHubToDTEvents")]
        public async void Run([EventGridTrigger]EventGridEvent eventGridEvent, ILogger log)
        {
            // After this is deployed, you need to turn the Managed Identity Status to "On",
            // Grab Object Id of the function and assigned "Azure Digital Twins Owner (Preview)" role
            // to this function identity in order for this function to be authorized on ADT APIs.
            log.LogInformation($"---------------------------------------------------------------------------------------");
            log.LogInformation($"ProcessHubToDTEvents Fired!");
            //Authenticate with Digital Twins
            var credentials = new DefaultAzureCredential();
            DigitalTwinsClient client = new DigitalTwinsClient(
                new Uri(adtServiceUrl), credentials, new DigitalTwinsClientOptions
                { Transport = new HttpClientTransport(httpClient) });
            log.LogInformation($"ADT service client connection created. URL: {adtServiceUrl}"); 

            if (eventGridEvent != null && eventGridEvent.Data != null)
            {
                try
                {
                    log.LogInformation(eventGridEvent.Data.ToString());

                    // Reading deviceId and temperature for IoT Hub JSON
                    JObject deviceMessage = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());
                    string deviceId = (string)deviceMessage["systemProperties"]["iothub-connection-device-id"];
                    var type = deviceMessage["body"]["Type"];
                    var moisture = deviceMessage["body"]["Moisture"];
                    var uv = deviceMessage["body"]["UV"];

                    log.LogInformation($"Device: {deviceId}. Moisture is:{moisture}. Type is:{type}. UV is:{uv}. ");
                    await FetchAndPrintTwinAsync(deviceId, client, log);

                    //Update twin using device type, moisture and UV
                    var updateTwinData = new JsonPatchDocument();
                    updateTwinData.AppendReplace("/Type", type.Value<string>());
                    updateTwinData.AppendReplace("/Moisture", moisture.Value<double>());
                    updateTwinData.AppendReplace("/UV", uv.Value<double>());
                    log.LogInformation($"Updating twin with {updateTwinData}");
                    var response = await client.UpdateDigitalTwinAsync(deviceId, updateTwinData);
                    log.LogInformation($"Response {response.ReasonPhrase} {response.Content.ToString()}");
                }
                catch (Exception ex)
                {
                    log.LogError($"ADT Hub to DT failed with message: {ex.Message}");
                }
            }
        }

        private async Task<BasicDigitalTwin> FetchAndPrintTwinAsync(string twinId, DigitalTwinsClient client, ILogger log)
        {
            Response<BasicDigitalTwin> twinResponse = await client.GetDigitalTwinAsync<BasicDigitalTwin>(twinId);
            var twin = twinResponse.Value;

            log.LogInformation($"Model id: {twin.Metadata.ModelId}");
            foreach (string prop in twin.Contents.Keys)
            {
                if (twin.Contents.TryGetValue(prop, out object value))
                    log.LogInformation($"Property '{prop}': {value}");
            }

            return twin;
        }
    }
}

