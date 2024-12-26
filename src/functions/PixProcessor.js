// PixProcessor.js

const { app } = require("@azure/functions");
const { EventHubConsumerClient } = require("@azure/event-hubs");
const { BlobServiceClient } = require("@azure/storage-blob");
const sharp = require("sharp");

// Azure Event Hub and Storage connection details

const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
const CONTAINER_NAME = "azure-ascendants-melty-magic-thumbnails";

// Function to process each event message
async function processEvent(eventData, context) {
  const { name, brand, blobUrl } = eventData;

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

    context.log(`Thumbnail created and uploaded: ${blockBlobClient.url}`);
  } catch (error) {
    context.log(`Error processing image: ${error.message}`);
  }
}

// Azure Function app entry point
app.eventHub("PixProcessor", {
  connection: "EVENT_HUB_CONNECTION",
  eventHubName: "chocolates",
  cardinality: "many", // Batch of messages
  handler: async (messages, context) => {
    context.log(
      `Event hub function processed ${messages.length} messages`
    );

    for (const message of messages) {
      context.log("Processing message:", message);
      await processEvent(message, context);
    }
  },
});
