using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Microsoft.Azure.EventGrid;
using Microsoft.Azure.EventGrid.Models;

namespace EventReceiver
{
    class Program
    {
        private const string ehubNamespaceConnectionString = "XXX";
        //private const string eventHubName = "hub01";
        private const string eventHubName = "hub02";
        private const string blobStorageConnectionString = "XXX";
        private const string blobContainerName = "checkpoints";
        static async Task Main()
        {
            // Read all environment variables
            // Event Hub processor needs its own blob container (to store checkpoints, etc.)
            string ehubNamespaceConnStr = Environment.GetEnvironmentVariable("EHUB_NS_CONNSTR");
            string eventProcBlobConnStr = Environment.GetEnvironmentVariable("EHUB_BLOB_CONNSTR");
            string eHub = Environment.GetEnvironmentVariable("EHUB");
            string eventProcContainer = Environment.GetEnvironmentVariable("EHUB_CONTAINER");

            // Read from the default consumer group: $Default
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            // Create a blob container client that the event processor will use 
            BlobContainerClient eventProcStorageClient = new BlobContainerClient(eventProcBlobConnStr, eventProcContainer);

            // Create an event processor client to process events in the event hub
            EventProcessorClient processor = new EventProcessorClient(eventProcStorageClient, consumerGroup, ehubNamespaceConnStr, eHub);

            // Register handlers for processing events and handling errors
            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            // Start the processing
            await processor.StartProcessingAsync();

            // Wait for 10 seconds for the events to be processed
            await Task.Delay(TimeSpan.FromSeconds(1000));

            // Stop the processing
            await processor.StopProcessingAsync();
        }
        static Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tRecevied event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

            EventGridSubscriber eventGridSubscriber = new EventGridSubscriber();
            EventGridEvent[] eventGridEvents = eventGridSubscriber.DeserializeEventGridEvents(Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));
            foreach (EventGridEvent eventGridEvent in eventGridEvents)
            {
                // All possible events: https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.eventgrid.models?view=azure-dotnet
                // We are only processing events for "StorageBlobCreatedEventData"
                if (eventGridEvent.Data is StorageBlobCreatedEventData)
                {
                    DateTime msgUtcNow = DateTime.UtcNow;
                    Console.WriteLine($"RCVD: Event ID:{eventGridEvent.Id} [Latency:{msgUtcNow.Subtract(eventGridEvent.EventTime)}]");
                    var eventData = (StorageBlobCreatedEventData)eventGridEvent.Data;
                    Console.WriteLine($"Got BlobCreated event data, blob URI {eventData.Url}");
                }
            }

            return Task.CompletedTask;
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{ eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}
