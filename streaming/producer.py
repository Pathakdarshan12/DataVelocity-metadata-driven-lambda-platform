#!/usr/bin/env python3
"""
Kafka Producer for DATAVELOCITY Streaming Pipeline
Generates test data for orders, order-items, and delivery events
"""

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPICS = {
    'orders': 'orders-events',
    'order_items': 'order-items-events',
    'delivery': 'delivery-events'
}

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8') if k else None,
    acks='all',
    retries=3
)

# Sample data
CUSTOMER_IDS = list(range(1, 101))  # 100 customers
RESTAURANT_IDS = list(range(1, 51))  # 50 restaurants
MENU_IDS = list(range(1, 201))  # 200 menu items
DELIVERY_AGENT_IDS = list(range(1, 31))  # 30 agents
ADDRESS_IDS = list(range(1, 151))  # 150 addresses

ORDER_STATUSES = ['PLACED', 'CONFIRMED', 'PREPARING', 'READY', 'DELIVERED', 'CANCELLED']
DELIVERY_STATUSES = ['ASSIGNED', 'PICKED_UP', 'IN_TRANSIT', 'DELIVERED']
PAYMENT_METHODS = ['CARD', 'UPI', 'CASH', 'WALLET']

# Counters
order_counter = 1000
order_item_counter = 5000
delivery_counter = 2000


def generate_order_event(event_type='ORDER_CREATED'):
    """Generate order event"""
    global order_counter
    order_counter += 1

    order = {
        'event_type': event_type,
        'event_timestamp': datetime.now().isoformat(),
        'order_id': order_counter,
        'customer_id': random.choice(CUSTOMER_IDS),
        'restaurant_id': random.choice(RESTAURANT_IDS),
        'order_date': datetime.now().isoformat(),
        'total_amount': round(random.uniform(100, 2000), 2),
        'status': 'PLACED' if event_type == 'ORDER_CREATED' else random.choice(ORDER_STATUSES),
        'payment_method': random.choice(PAYMENT_METHODS)
    }

    return order


def generate_order_items(order_id, num_items=None):
    """Generate order items for an order"""
    global order_item_counter

    if num_items is None:
        num_items = random.randint(1, 5)

    items = []
    for _ in range(num_items):
        order_item_counter += 1
        price = round(random.uniform(50, 500), 2)
        quantity = random.randint(1, 3)

        item = {
            'event_type': 'ITEM_ADDED',
            'event_timestamp': datetime.now().isoformat(),
            'order_item_id': order_item_counter,
            'order_id': order_id,
            'menu_id': random.choice(MENU_IDS),
            'quantity': quantity,
            'price': price,
            'subtotal': round(price * quantity, 2)
        }
        items.append(item)

    return items


def generate_delivery_event(order_id, event_type='DELIVERY_ASSIGNED'):
    """Generate delivery event"""
    global delivery_counter

    if event_type == 'DELIVERY_ASSIGNED':
        delivery_counter += 1
        delivery_id = delivery_counter
    else:
        delivery_id = delivery_counter

    delivery = {
        'event_type': event_type,
        'event_timestamp': datetime.now().isoformat(),
        'delivery_id': delivery_id,
        'order_id': order_id,
        'delivery_agent_id': random.choice(DELIVERY_AGENT_IDS),
        'delivery_status': 'ASSIGNED' if event_type == 'DELIVERY_ASSIGNED' else random.choice(DELIVERY_STATUSES),
        'estimated_time': (datetime.now() + timedelta(minutes=random.randint(20, 60))).isoformat(),
        'customer_address_id': random.choice(ADDRESS_IDS),
        'delivery_date': datetime.now().isoformat() if event_type == 'DELIVERY_COMPLETED' else None,
        'location': {
            'latitude': round(random.uniform(18.4, 18.6), 6),
            'longitude': round(random.uniform(73.8, 74.0), 6)
        }
    }

    return delivery


def send_event(topic, key, value):
    """Send event to Kafka"""
    try:
        future = producer.send(topic, key=key, value=value)
        result = future.get(timeout=10)
        print(f"✓ Sent to {topic}: {value['event_type']} (offset: {result.offset})")
        return True
    except KafkaError as e:
        print(f"✗ Failed to send to {topic}: {e}")
        return False


def simulate_order_lifecycle():
    """Simulate complete order lifecycle"""
    print("\n" + "=" * 60)
    print("SIMULATING ORDER LIFECYCLE")
    print("=" * 60)

    # 1. Create order
    order = generate_order_event('ORDER_CREATED')
    send_event(TOPICS['orders'], order['order_id'], order)
    time.sleep(0.5)

    # 2. Add order items
    items = generate_order_items(order['order_id'], num_items=random.randint(2, 4))
    for item in items:
        send_event(TOPICS['order_items'], item['order_item_id'], item)
        time.sleep(0.3)

    # 3. Assign delivery
    delivery = generate_delivery_event(order['order_id'], 'DELIVERY_ASSIGNED')
    send_event(TOPICS['delivery'], delivery['delivery_id'], delivery)
    time.sleep(0.5)

    # 4. Update order status
    order['status'] = 'CONFIRMED'
    order['event_type'] = 'ORDER_UPDATED'
    order['event_timestamp'] = datetime.now().isoformat()
    send_event(TOPICS['orders'], order['order_id'], order)
    time.sleep(1)

    # 5. Update delivery status
    delivery['event_type'] = 'STATUS_UPDATED'
    delivery['delivery_status'] = 'PICKED_UP'
    delivery['event_timestamp'] = datetime.now().isoformat()
    send_event(TOPICS['delivery'], delivery['delivery_id'], delivery)
    time.sleep(2)

    # 6. Complete delivery
    delivery['event_type'] = 'DELIVERY_COMPLETED'
    delivery['delivery_status'] = 'DELIVERED'
    delivery['delivery_date'] = datetime.now().isoformat()
    delivery['event_timestamp'] = datetime.now().isoformat()
    send_event(TOPICS['delivery'], delivery['delivery_id'], delivery)

    # 7. Final order update
    order['status'] = 'DELIVERED'
    order['event_type'] = 'ORDER_UPDATED'
    order['event_timestamp'] = datetime.now().isoformat()
    send_event(TOPICS['orders'], order['order_id'], order)

    print(f"\n✓ Order {order['order_id']} lifecycle completed")


def continuous_stream(num_orders=10, delay=5):
    """Generate continuous stream of orders"""
    print("\n" + "=" * 60)
    print(f"STARTING CONTINUOUS STREAM ({num_orders} orders)")
    print("=" * 60)

    for i in range(num_orders):
        print(f"\n--- Order {i + 1}/{num_orders} ---")
        simulate_order_lifecycle()

        if i < num_orders - 1:
            print(f"\nWaiting {delay} seconds before next order...")
            time.sleep(delay)

    print("\n" + "=" * 60)
    print("STREAM COMPLETED")
    print("=" * 60)


def main():
    """Main function"""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║   DATAVELOCITY Kafka Streaming Producer                  ║
    ║   Real-time Order, Order Items & Delivery Events         ║
    ╚══════════════════════════════════════════════════════════╝
    """)

    try:
        print("\n📡 Connecting to Kafka broker...")
        print(f"   Broker: {KAFKA_BROKER}")
        print(f"   Topics: {', '.join(TOPICS.values())}")

        # Test connection
        producer.send(TOPICS['orders'], key=None, value={'test': 'connection'})
        producer.flush()
        print("✓ Connected successfully!\n")

        while True:
            print("\nSelect option:")
            print("1. Simulate single order lifecycle")
            print("2. Generate continuous stream (10 orders)")
            print("3. Generate continuous stream (100 orders)")
            print("4. Exit")

            choice = input("\nEnter choice (1-4): ").strip()

            if choice == '1':
                simulate_order_lifecycle()
            elif choice == '2':
                continuous_stream(num_orders=10, delay=5)
            elif choice == '3':
                continuous_stream(num_orders=100, delay=2)
            elif choice == '4':
                print("\n👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice")

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        print("\n🔒 Closing producer...")
        producer.close()
        print("✓ Producer closed")


if __name__ == "__main__":
    main()