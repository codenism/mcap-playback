import sys
import asyncio
import os
from mcap.reader import make_reader
from foxglove_websocket import run_cancellable
from foxglove_websocket.server import FoxgloveServer, FoxgloveServerListener
from foxglove_websocket.types import ChannelId
import json
import base64

async def main():
    class Listener(FoxgloveServerListener):
        async def on_subscribe(self, server: FoxgloveServer, channel_id: ChannelId):
            print(f"Client subscribed to channel {channel_id}")

        async def on_unsubscribe(self, server: FoxgloveServer, channel_id: ChannelId):
            print(f"Client unsubscribed from channel {channel_id}")

    mcap_file = "/data/input.mcap"

    if not os.path.exists(mcap_file):
        print(f"Error: File {mcap_file} not found")
        return

    async with FoxgloveServer("0.0.0.0", 8765, "mcap-server") as server:
        server.set_listener(Listener())
        try:
            with open(mcap_file, "rb") as f:
                reader = make_reader(f)

                # Сбор информации о каналах и схемах
                channels_info = {}
                schemas_info = {}
                messages = []

                for schema, channel, message in reader.iter_messages():
                    if schema and schema.id not in schemas_info:
                        schemas_info[schema.id] = {
                            "name": schema.name,
                            "encoding": schema.encoding,
                            "data": schema.data
                        }
                    if channel and channel.id not in channels_info:
                        channels_info[channel.id] = {
                            "topic": channel.topic,
                            "message_encoding": channel.message_encoding,
                            "schema_id": channel.schema_id,
                            "metadata": channel.metadata
                        }
                    if message:
                        messages.append((channel.id, message))

                # Регистрация каналов
                foxglove_channels = {}
                for channel_id, channel in channels_info.items():
                    schema = schemas_info.get(channel["schema_id"])

                    # Подготовка данных схемы
                    schema_data = ""
                    schema_encoding = "json"  # По умолчанию для Lightblick Suite
                    if schema:
                        try:
                            if schema["encoding"] == "jsonschema":
                                schema_data = schema["data"].decode("utf-8")
                                json.loads(schema_data)  # Валидация JSON
                                schema_encoding = "json"
                            elif schema["encoding"] == "protobuf":
                                schema_data = base64.b64encode(schema["data"]).decode("utf-8")
                                schema_encoding = "protobuf"
                            else:
                                schema_data = base64.b64encode(schema["data"]).decode("utf-8")
                                schema_encoding = schema["encoding"] or "raw"
                        except (UnicodeDecodeError, json.JSONDecodeError) as e:
                            print(f"Schema processing error for {channel['topic']}: {e}")
                            schema_data = base64.b64encode(schema["data"]).decode("utf-8")
                            schema_encoding = "raw"

                    channel_params = {
                        "topic": channel["topic"],
                        "encoding": channel["message_encoding"] or "json",
                        "schemaName": schema["name"] if schema else channel["topic"],
                        "schema": schema_data,
                        "schemaEncoding": schema_encoding,
                    }

                    print(f"Registering channel: {channel['topic']} (encoding: {channel_params['encoding']}, schema: {channel_params['schemaName']})")

                    try:
                        chan_id = await server.add_channel(channel_params)
                        foxglove_channels[channel_id] = chan_id
                        print(f"Successfully registered channel {channel['topic']} (ID: {chan_id})")
                    except Exception as e:
                        print(f"Failed to register channel {channel['topic']}: {str(e)}")
                        continue

                # Воспроизведение сообщений
                prev_time = None
                total = len(messages)
                for i, (channel_id, message) in enumerate(messages):
                    if channel_id not in foxglove_channels:
                        continue

                    channel_info = channels_info[channel_id]

                    # Обработка временных меток
                    if prev_time is not None:
                        delay = (message.log_time - prev_time) / 1e9
                        try:
                            await asyncio.sleep(max(delay, 0))
                        except asyncio.CancelledError:
                            print("Playback cancelled")
                            break

                    # Обработка данных сообщения
                    try:
                        message_data = message.data
                        if channel_info["message_encoding"] == "json":
                            try:
                                # Для JSON-сообщений пробуем декодировать
                                message_data = message.data.decode("utf-8")
                                json.loads(message_data)  # Валидация
                            except (UnicodeDecodeError, json.JSONDecodeError):
                                message_data = base64.b64encode(message.data).decode("utf-8")
                        elif channel_info["message_encoding"] == "protobuf":
                            # Для Protobuf отправляем как есть
                            message_data = message.data
                        else:
                            # Для других форматов — бинарные данные
                            message_data = message.data

                        await server.send_message(
                            foxglove_channels[channel_id],
                            message.log_time,
                            message_data
                        )

                        print(f"Sent {i+1}/{total} to {channel_info['topic']} [{len(message.data)} bytes]", end="\r")

                    except Exception as e:
                        print(f"\nFailed to send message on {channel_info['topic']}: {str(e)}")
                        continue

                    prev_time = message.log_time

        except Exception as e:
            print(f"Server error: {str(e)}")
        finally:
            print("\nServer stopped gracefully")

if __name__ == "__main__":
    run_cancellable(main())