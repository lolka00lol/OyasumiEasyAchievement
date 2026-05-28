import asyncio
import json
import logging
from amqtt.broker import Broker
from amqtt.client import MQTTClient
from amqtt.mqtt.constants import QOS_0

# Отключаем лишние логи, чтобы видеть только важное
logging.getLogger('amqtt').setLevel(logging.ERROR)

# Минимальный конфиг без плагинов, которые вызывают ошибку
config = {
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': '127.0.0.1:1883',
        },
        'ws': {
            'type': 'ws',
            'bind': '127.0.0.1:9001', # Порт для OyasumiVR
        }
    }
}

async def ha_logic():
    """Логика имитации Home Assistant"""
    await asyncio.sleep(2) # Даем брокеру время запуститься
    
    client = MQTTClient()
    try:
        # Подключаемся к своему же брокеру
        await client.connect('mqtt://127.0.0.1:1883/')
        # Слушаем ВСЕ сообщения от OyasumiVR
        await client.subscribe([
            ('homeassistant/+/OyasumiVR/+/config', QOS_0),
            ('OyasumiVR/#', QOS_0)
        ])
        
        print("🤖 [HA Simulation] Logic enabled. Waiting OyasumiVR...")
        
        while True:
            message = await client.deliver_message()
            packet = message.publish_packet
            topic = packet.variable_header.topic_name
            payload = packet.payload.data.decode()
            
            # Если видим конфиг устройства
            if "/config" in topic:
                try:
                    data = json.loads(payload)
                    name = data.get("name", "Device")
                    cmd_topic = data.get("command_topic")
                    
                    print(f"✅ Обнаружено: {name}")
                    
                    if cmd_topic:
                        print(f"🚀 Simulate disabling (OFF) for {name}...")
                        await client.publish(cmd_topic, b"OFF", qos=QOS_0)
                        
                        # Подтверждаем статус
                        state_topic = data.get("state_topic")
                        if state_topic:
                            await client.publish(state_topic, b"OFF", qos=QOS_0)
                except:
                    pass
                    
    except Exception as e:
        print(f"❌ Error in logic: {e}")

async def main():
    # Создаем брокер без вызова проблемных плагинов
    broker = Broker(config)
    print("🌐 [Broker] Server enabled on 127.0.0.1:9001 (WS)...")
    
    try:
        await broker.start()
        print("🟢 [Broker] Server working!")
        await ha_logic()
    except Exception as e:
        print(f"❌ Critical error: {e}")
    finally:
        await broker.shutdown()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStoping...")