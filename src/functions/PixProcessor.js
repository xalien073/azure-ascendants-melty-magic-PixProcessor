// PixProcessor.js

const { app } = require("@azure/functions");
const { EventHubConsumerClient } = require("@azure/event-hubs");
const { BlobServiceClient } = require("@azure/storage-blob");
const { TableClient, AzureNamedKeyCredential } = require("@azure/data-tables");
const { EventGridPublisherClient, AzureKeyCredential } = require("@azure/eventgrid");
const sharp = require("sharp");

// Azure Event Hub and Storage connection details
const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
const CONTAINER_NAME = "azure-ascendants-melty-magic-thumbnails";
const TABLE_ACCOUNT_NAME = "vipernest";
const TABLE_ACCOUNT_KEY = process.env.TABLE_ACCOUNT_KEY; // Only the Account Key
const TABLE_NAME = "azureAscendantsMeltyMagicInventory";

// Azure Event Grid Custom Topic details
const EVENT_GRID_TOPIC_ENDPOINT = process.env.EVENT_GRID_TOPIC_ENDPOINT;
const EVENT_GRID_TOPIC_KEY = process.env.EVENT_GRID_TOPIC_KEY;

// Initialize Azure Table Storage Client
const credential = new AzureNamedKeyCredential(TABLE_ACCOUNT_NAME, TABLE_ACCOUNT_KEY);
const tableClient = new TableClient(
  `https://${TABLE_ACCOUNT_NAME}.table.core.windows.net`,
  TABLE_NAME,
  credential
);

// Initialize Azure Event Grid Publisher Client
const eventGridClient = new EventGridPublisherClient(
  EVENT_GRID_TOPIC_ENDPOINT,
  "EventGrid",
  new AzureKeyCredential(EVENT_GRID_TOPIC_KEY)
);

// Function to process each event message
async function processEvent(eventData, context) {
  const { name, brand, price, quantityAvailable, blobUrl } = eventData;

  if (!blobUrl) {
    context.log("No blob URL provided");
    return;
  }

  try {
    // Fetch the image from the blob URL
    const fetch = (await import("node-fetch")).default;
    const response = await fetch(blobUrl);
    const arrayBuffer = await response.arrayBuffer();
    const imageBuffer = Buffer.from(arrayBuffer);

    // Check if the buffer is a valid image
    const metadata = await sharp(imageBuffer).metadata();
    if (!metadata.format) {
      context.log("Unsupported image format");
      return;
    }

    // Resize the image using Sharp
    const thumbnailBuffer = await sharp(imageBuffer)
      .resize(150, 150)
      .toBuffer();

    // Upload the thumbnail to Azure Blob Storage
    const blobServiceClient = BlobServiceClient.fromConnectionString(
      AZURE_STORAGE_CONNECTION_STRING
    );
    const containerClient = blobServiceClient.getContainerClient(CONTAINER_NAME);
    const thumbnailName = `${Date.now()}-thumbnail.jpg`;
    const blockBlobClient = containerClient.getBlockBlobClient(thumbnailName);

    await blockBlobClient.uploadData(thumbnailBuffer, {
      blobHTTPHeaders: { blobContentType: "image/jpeg" },
    });

    const thumbnailUrl = blockBlobClient.url;

    // Save metadata to Azure Table Storage
    const entity = {
      partitionKey: brand,
      rowKey: name,
      name,
      brand,
      price: parseFloat(price),
      quantityAvailable: parseInt(quantityAvailable, 10),
      imageUrl: thumbnailUrl, // Add the thumbnail URL to the metadata,
    };

    await tableClient.createEntity(entity);
    context.log("Product data saved successfully!");
    context.log(`Thumbnail created and uploaded: ${thumbnailUrl}`);

    // Send an event to Event Grid
    const event = {
      eventType: "ProductCreated",
      subject: `Products/${brand}/${name}`,
      dataVersion: "1.0",
      data: {
        name,
        brand,
        price: parseFloat(price),
        quantityAvailable: parseInt(quantityAvailable, 10),
        imageUrl: thumbnailUrl,
      },
    };

    await eventGridClient.send([event]);
    context.log("Event published to Event Grid successfully!");
  } catch (error) {
    context.log(`Error processing data: ${error.message}`);
  }
}

// Azure Function app entry point
app.eventHub("PixProcessor", {
  connection: "EVENT_HUB_CONNECTION",
  eventHubName: "chocolates",
  cardinality: "many", // Batch of messages
  handler: async (messages, context) => {
    context.log(`Event hub function processed ${messages.length} messages`);

    for (const message of messages) {
      context.log("Processing message:", message);
      await processEvent(message, context);
    }
  },
});


// // PixProcessor.js

// const { app } = require("@azure/functions");
// const { EventHubConsumerClient } = require("@azure/event-hubs");
// const { BlobServiceClient } = require("@azure/storage-blob");
// const { TableClient, AzureNamedKeyCredential } = require("@azure/data-tables");
// const sharp = require("sharp");

// // Azure Event Hub and Storage connection details
// const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
// const CONTAINER_NAME = "azure-ascendants-melty-magic-thumbnails";
// const TABLE_ACCOUNT_NAME = "vipernest";
// const TABLE_ACCOUNT_KEY = process.env.TABLE_ACCOUNT_KEY; // Only the Account Key
// const TABLE_NAME = "azureAscendantsMeltyMagicInventory";

// // Initialize Azure Table Storage Client
// const credential = new AzureNamedKeyCredential(TABLE_ACCOUNT_NAME, TABLE_ACCOUNT_KEY);
// const tableClient = new TableClient(
//   `https://${TABLE_ACCOUNT_NAME}.table.core.windows.net`,
//   TABLE_NAME,
//   credential
// );

// // Function to process each event message
// async function processEvent(eventData, context) {
//   const { name, brand, price, quantityAvailable, blobUrl } = eventData;

//   if (!blobUrl) {
//     context.log("No blob URL provided");
//     return;
//   }

//   try {
//     // Fetch the image from the blob URL
//     const fetch = (await import("node-fetch")).default;
//     const response = await fetch(blobUrl);
//     const arrayBuffer = await response.arrayBuffer();
//     const imageBuffer = Buffer.from(arrayBuffer);

//     // Check if the buffer is a valid image
//     const metadata = await sharp(imageBuffer).metadata();
//     if (!metadata.format) {
//       context.log("Unsupported image format");
//       return;
//     }

//     // Resize the image using Sharp
//     const thumbnailBuffer = await sharp(imageBuffer)
//       .resize(150, 150)
//       .toBuffer();

//     // Upload the thumbnail to Azure Blob Storage
//     const blobServiceClient = BlobServiceClient.fromConnectionString(
//       AZURE_STORAGE_CONNECTION_STRING
//     );
//     const containerClient = blobServiceClient.getContainerClient(CONTAINER_NAME);
//     const thumbnailName = `${Date.now()}-thumbnail.jpg`;
//     const blockBlobClient = containerClient.getBlockBlobClient(thumbnailName);

//     await blockBlobClient.uploadData(thumbnailBuffer, {
//       blobHTTPHeaders: { blobContentType: "image/jpeg" },
//     });

//     const thumbnailUrl = blockBlobClient.url;

//     // Save metadata to Azure Table Storage
//     const entity = {
//       partitionKey: brand,
//       rowKey: name,
//       name,
//       brand,
//       price: parseFloat(price),
//       quantityAvailable: parseInt(quantityAvailable, 10),
//       imageUrl: thumbnailUrl, // Add the thumbnail URL to the metadata,
//     };

//     await tableClient.createEntity(entity);
//     context.log("Product data saved successfully!");
//     context.log(`Thumbnail created and uploaded: ${thumbnailUrl}`);
//   } catch (error) {
//     context.log(`Error processing image: ${error.message}`);
//   }
// }

// // Azure Function app entry point
// app.eventHub("PixProcessor", {
//   connection: "EVENT_HUB_CONNECTION",
//   eventHubName: "chocolates",
//   cardinality: "many", // Batch of messages
//   handler: async (messages, context) => {
//     context.log(`Event hub function processed ${messages.length} messages`);

//     for (const message of messages) {
//       context.log("Processing message:", message);
//       await processEvent(message, context);
//     }
//   },
// });
