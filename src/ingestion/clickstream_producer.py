"""
Kafka Producer: Generate simulated clickstream events
"""
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.db_connection import DatabaseConnection

class ClickstreamGenerator:
    """Generates realistic clickstream events"""
    
    def __init__(self):
        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Load real product IDs from database
        self.product_ids = self.load_product_ids()
        self.customer_ids = self.load_customer_ids()
        
        # Event types with probabilities
        self.event_types = {
            'view': 0.70,      # 70% views
            'add_to_cart': 0.20,  # 20% add to cart
            'purchase': 0.10    # 10% purchases
        }
        
        print(f"✅ Clickstream Generator initialized")
        print(f"   📦 Loaded {len(self.product_ids)} products")
        print(f"   👤 Loaded {len(self.customer_ids)} customers")
    
    def load_product_ids(self):
        """Load real product IDs from database"""
        with DatabaseConnection() as db:
            result = db.fetch_all("SELECT product_id FROM dim_products LIMIT 1000")
            return [row[0] for row in result]
    
    def load_customer_ids(self):
        """Load real customer IDs from database"""
        with DatabaseConnection() as db:
            result = db.fetch_all("SELECT customer_id FROM dim_customers LIMIT 5000")
            return [row[0] for row in result]
    
    def generate_event(self):
        """Generate a single clickstream event"""
        
        # Choose event type based on probability
        rand = random.random()
        if rand < 0.70:
            event_type = 'view'
        elif rand < 0.90:
            event_type = 'add_to_cart'
        else:
            event_type = 'purchase'
        
        event = {
            'event_id': f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            'event_type': event_type,
            'customer_id': random.choice(self.customer_ids),
            'product_id': random.choice(self.product_ids),
            'timestamp': datetime.now().isoformat(),
            'session_id': f"session_{random.randint(10000, 99999)}",
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'page_url': f"/product/{random.randint(1, 1000)}",
            'referrer': random.choice(['google', 'facebook', 'direct', 'email', 'instagram'])
        }
        
        # Add price for purchase events
        if event_type == 'purchase':
            event['price'] = round(random.uniform(10, 500), 2)
            event['quantity'] = random.randint(1, 3)
        
        return event
    
    def start(self, events_per_second=10, duration_seconds=60):
        """Start generating events"""
        print(f"\n🚀 Starting clickstream generation...")
        print(f"   Rate: {events_per_second} events/second")
        print(f"   Duration: {duration_seconds} seconds")
        print(f"   Total events: ~{events_per_second * duration_seconds}")
        print("\n   Press Ctrl+C to stop early\n")
        
        event_count = 0
        start_time = time.time()
        
        try:
            while time.time() - start_time < duration_seconds:
                for _ in range(events_per_second):
                    event = self.generate_event()
                    
                    # Send to Kafka
                    self.producer.send('clickstream_events', value=event)
                    event_count += 1
                    
                    # Print progress every 100 events
                    if event_count % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = event_count / elapsed
                        print(f"   📊 {event_count:,} events sent | {rate:.1f} events/sec | Type: {event['event_type']}")
                
                time.sleep(1)  # Wait 1 second before next batch
        
        except KeyboardInterrupt:
            print("\n⚠️  Interrupted by user")
        
        finally:
            # Flush and close
            self.producer.flush()
            self.producer.close()
            
            elapsed = time.time() - start_time
            avg_rate = event_count / elapsed if elapsed > 0 else 0
            
            print(f"\n✅ Generation complete!")
            print(f"   Total events: {event_count:,}")
            print(f"   Duration: {elapsed:.1f} seconds")
            print(f"   Average rate: {avg_rate:.1f} events/second")


if __name__ == "__main__":
    generator = ClickstreamGenerator()
    
    # Generate events for 2 minutes at 10 events/second
    generator.start(events_per_second=10, duration_seconds=120)
