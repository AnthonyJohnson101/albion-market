import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import { connect, StringCodec } from 'nats';
import { MongoClient } from 'mongodb';
import fs from 'fs/promises';

// Get current directory path
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load environment variables
dotenv.config({ path: path.resolve(__dirname, '.env') });
if (!process.env.MONGO_URI) {
  console.error("❌ MONGO_URI is not defined in .env file");
  process.exit(1);
}

// Configuration
const MONGO_URI = process.env.MONGO_URI
const DB_NAME = "albion-market"
const COLLECTION_NAME = "items"
const ITEMS_FILE = "items.json";
const CHUNK_INTERVAL = 1000; // Process chunks every 1 second

// City ID to name mapping
const CITY_MAP = {
  "7": "Thetford Market",
  "1002": "Lymhurst Market",
  "2004": "Bridgewatch Market",
  "3005": "Caerleon Market",
  "3008": "Martlock Market",
  "4002": "Fort Sterling Market",
  "5003": "Brecilien Market" 
};

// Function to normalize item ID by removing enchantment suffix
const normalizeItemId = (itemId) => itemId.replace(/_LEVEL\d+/, '').replace(/@\d+$/, '');

// Item name lookup map
let itemNameMap = new Map();

(async () => {
  let mongoClient;
  let nc;
  let chunkCache = new Map(); // Temporary cache for current chunk
  let chunkTimeout; // Reference to the current chunk timeout

  try {

    // Load and parse items.json
    try {
      const itemsData = await fs.readFile(ITEMS_FILE, 'utf8');
      const items = JSON.parse(itemsData);
      
      for (const item of items) {
        // Use UniqueName as key and English name as value
        if (item.UniqueName && item.LocalizedNames?.["EN-US"]) {
          itemNameMap.set(item.UniqueName, item.LocalizedNames["EN-US"]);
        }
      }
      console.log(`✅ Loaded ${itemNameMap.size} item names from ${ITEMS_FILE}`);
    } catch (err) {
      console.error(`⚠️ Error loading ${ITEMS_FILE}:`, err);
      // Continue without names if file can't be loaded
    }

    // Connect to MongoDB
    mongoClient = new MongoClient(MONGO_URI);
    await mongoClient.connect();
    console.log("✅ Connected to MongoDB");
    const db = mongoClient.db(DB_NAME);
    const bestPricesCollection = db.collection(COLLECTION_NAME);

    // Connect to NATS
    nc = await connect({
      servers: "nats.albion-online-data.com:4222",
      user: "public",
      pass: "thenewalbiondata"
    });
    console.log("✅ Connected to NATS");

    // Function to generate composite key
    const getCompositeKey = (item) => {
      const baseItemId = normalizeItemId(item.ItemTypeId);
      return `${baseItemId}_${item.QualityLevel}_${item.EnchantmentLevel}_${item.LocationId}`;
    };

    // Process current chunk and reset cache
    const processChunk = async () => {
      if (chunkCache.size === 0) {
        // Schedule next chunk processing
        chunkTimeout = setTimeout(processChunk, CHUNK_INTERVAL);
        return;
      }

      const currentChunk = new Map(chunkCache);
      chunkCache.clear();

      try {
        const bulkOps = [];
        for (const [compositeKey, bestOrder] of currentChunk) {
          const item = bestOrder;
          const cityName = CITY_MAP[item.LocationId] || `Unknown (${item.LocationId})`;
          const baseItemId = normalizeItemId(item.ItemTypeId);
          // Get English name (fallback to baseItemId if not found)
          const englishName = itemNameMap.get(item.ItemTypeId) || baseItemId;
          
          bulkOps.push({
            updateOne: {
              filter: { composite_key: compositeKey },
              update: { 
                $set: { 
                  item_id: baseItemId,
                  item_name: englishName,
                  quality: item.QualityLevel,
                  enchantment: item.EnchantmentLevel,
                  best_price: item.UnitPriceSilver,
                  last_updated: new Date(),
                  tier: baseItemId.split('_')[0].substring(1),
                  location_id: item.LocationId,
                  city: cityName,
                  full_item_id: item.ItemTypeId
                }
              },
              upsert: true
            }
          });
        }
        
        if (bulkOps.length > 0) {
          const result = await bestPricesCollection.bulkWrite(bulkOps);
          console.log(`💾 Updated ${bulkOps.length} items (Matched: ${result.matchedCount}, Upserted: ${result.upsertedCount})`);
        }
      } catch (err) {
        console.error('⚠️ Bulk update error:', err);
      } finally {
        // Schedule next chunk processing
        chunkTimeout = setTimeout(processChunk, CHUNK_INTERVAL);
      }
    };

    // Start chunk processing
    chunkTimeout = setTimeout(processChunk, CHUNK_INTERVAL);

    // Graceful shutdown handler
    const shutdown = async () => {
      console.log("\n🚦 Shutting down...");
      
      // Clear pending chunk processing
      clearTimeout(chunkTimeout);
      
      // Process any remaining items in final chunk
      if (chunkCache.size > 0) {
        console.log("⏳ Processing final chunk...");
        const currentChunk = new Map(chunkCache);
        chunkCache.clear();
        
        for (const [_, bestOrder] of currentChunk) {
          const compositeKey = getCompositeKey(bestOrder);
          const cityName = CITY_MAP[bestOrder.LocationId] || `Unknown (${bestOrder.LocationId})`;
          const baseItemId = normalizeItemId(bestOrder.ItemTypeId);
          const englishName = itemNameMap.get(bestOrder.ItemTypeId) || baseItemId;
          
          try {
            await bestPricesCollection.updateOne(
              { composite_key: compositeKey },
              { $set: { 
                item_id: baseItemId,
                item_name: englishName,
                quality: bestOrder.QualityLevel,
                enchantment: bestOrder.EnchantmentLevel,
                best_price: bestOrder.UnitPriceSilver,
                last_updated: new Date(),
                tier: baseItemId.split('_')[0].substring(1),
                location_id: bestOrder.LocationId,
                city: cityName,
                full_item_id: bestOrder.ItemTypeId
              }},
              { upsert: true }
            );
          } catch (err) {
            console.error('⚠️ Final update error:', err);
          }
        }
      }
      
      if (nc) {
        console.log("⏳ Draining NATS connection...");
        await nc.drain();
        console.log("❌ NATS connection closed");
      }
      
      if (mongoClient) {
        console.log("⏳ Closing MongoDB connection...");
        await mongoClient.close();
        console.log("❌ MongoDB connection closed");
      }
      
      process.exit(0);
    };

    // Handle process termination signals
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    const sc = StringCodec();
    const sub = nc.subscribe("marketorders.deduped");

    // Message processing
    (async () => {
      for await (const m of sub) {
        try {
          const order = JSON.parse(sc.decode(m.data));
          
          // Only process sell orders
          if (order.AuctionType !== 'offer') continue;
          
          // Only process orders from specified cities
          if (!CITY_MAP.hasOwnProperty(order.LocationId.toString())) continue;
          
          // Parse expiration time
          const expires = new Date(order.Expires);
          const now = new Date();
          
          // Skip expired orders
          if (expires < now) continue;
          
          // Generate composite key
          const compositeKey = getCompositeKey(order);
          const currentBest = chunkCache.get(compositeKey);

          // Update chunk cache if no existing order or better price found
          if (!currentBest || order.UnitPriceSilver < currentBest.UnitPriceSilver) {
            chunkCache.set(compositeKey, order);
          }
          
        } catch (err) {
          console.error('⚠️ Message processing error:', err);
        }
      }
    })();

  } catch (err) {
    console.error("⚠️ Initialization error:", err);
    clearTimeout(chunkTimeout);
    if (mongoClient) await mongoClient.close();
    if (nc) await nc.drain();
    process.exit(1);
  }
})();