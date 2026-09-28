import { redis, DRIVER_GEO_KEY, setDriverStatus } from "./redis.js";

// Points scattered around central Ho Chi Minh City, for local testing.
const drivers = [
  { id: "driver-1", lat: 10.7769, lng: 106.7009 },
  { id: "driver-2", lat: 10.7797, lng: 106.6990 },
  { id: "driver-3", lat: 10.7720, lng: 106.6980 },
  { id: "driver-4", lat: 10.7830, lng: 106.7050 },
  { id: "driver-5", lat: 10.7700, lng: 106.7100 },
];

async function main() {
  await redis.del(DRIVER_GEO_KEY);

  for (const driver of drivers) {
    await redis.geoadd(DRIVER_GEO_KEY, driver.lng, driver.lat, driver.id);
    await setDriverStatus(driver.id, "AVAILABLE");
  }

  console.log(`Seeded ${drivers.length} AVAILABLE drivers into Redis`);
  await redis.quit();
}

main().catch((err) => {
  console.error("seed-drivers failed", err);
  process.exit(1);
});
