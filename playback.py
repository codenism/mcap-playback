# import asyncio
# import os
# from mcap.reader import make_reader
# from foxglove_websocket import run_cancellable
# from foxglove_websocket.server import FoxgloveServer, FoxgloveServerListener
# from foxglove_websocket.types import ChannelId
# import json
# import base64
# from typing import Optional
#
# class FoxgloveServerWithCapabilities(FoxgloveServer):
#     def __init__(self, host, port, name):
#         super().__init__(host, port, name)
#         self._capabilities = []
#         self._time_range = None
#         self._connections = set()
#
#     def set_capabilities(self, capabilities: list):
#         self._capabilities = capabilities
#         print(f"Set capabilities: {self._capabilities}")
#
#     def set_time_range(self, start: int, end: int):
#         self._time_range = (start, end)
#         print(f"Set time range: {start} to {end}")
#
#     async def add_connection(self, connection):
#         self._connections.add(connection)
#         print(f"Added connection: {connection}, Total: {len(self._connections)}")
#         client_id = hash(connection)
#         await self._send_server_info(client_id)
#
#     async def remove_connection(self, connection):
#         self._connections.discard(connection)
#         print(f"Removed connection: {connection}, Total: {len(self._connections)}")
#
#     async def send_json_to_all(self, msg: dict):
#         for connection in list(self._connections):
#             try:
#                 await connection.send(json.dumps(msg, separators=(",", ":")))
#                 print(f"Sent JSON to {connection}: {msg}")
#             except Exception as e:
#                 print(f"Failed to send message to {connection}: {str(e)}")
#                 await self.remove_connection(connection)
#
#     async def _send_server_info(self, client_id: int):
#         info = {
#             "op": "serverInfo",
#             "name": self.name,
#             "capabilities": self._capabilities,
#             "metadata": {
#                 "time": {
#                     "start": self._time_range[0] if self._time_range else 0,
#                     "end": self._time_range[1] if self._time_range else 0
#                 },
#                 "dataset": "mcap-local-file"
#             } if self._time_range else {
#                 "dataset": "mcap-local-file"
#             }
#         }
#         print(f"Sending serverInfo to client {client_id}: {info}")
#         await self._send_json(client_id, info)
#
#
# async def main():
#     class Listener(FoxgloveServerListener):
#         def __init__(self):
#             self.seek_time: Optional[int] = None
#             self.play: bool = True
#
#         async def on_subscribe(self, server: FoxgloveServer, channel_id: ChannelId):
#             print(f"Client subscribed to channel {channel_id}")
#
#         async def on_unsubscribe(self, server: FoxgloveServer, channel_id: ChannelId):
#             print(f"Client unsubscribed from channel {channel_id}")
#
#         async def on_client_message(self, server: FoxgloveServer, message: dict):
#             print(f"Received client message: {message}")
#             op = message.get("op")
#             if op == "seek":
#                 try:
#                     self.seek_time = int(message.get("time"))
#                     print(f"Received seek request to time: {self.seek_time}")
#                     if foxglove_channels:
#                         first_channel = list(foxglove_channels.values())[0]
#                         await server.send_message(
#                             first_channel,
#                             message.get("time", 0),
#                             json.dumps({
#                                 "op": "status",
#                                 "level": "info",
#                                 "message": f"Seeked to time {self.seek_time}"
#                             }).encode("utf-8")
#                         )
#                 except (TypeError, ValueError):
#                     print("Invalid seek time received")
#             elif op == "play":
#                 self.play = True
#                 print("Received play command")
#                 if foxglove_channels:
#                     first_channel = list(foxglove_channels.values())[0]
#                     await server.send_message(
#                         first_channel,
#                         start_time or 0,
#                         json.dumps({
#                             "op": "status",
#                             "level": "info",
#                             "message": "Playback started"
#                         }).encode("utf-8")
#                     )
#             elif op == "pause":
#                 self.play = False
#                 print("Received pause command")
#                 if foxglove_channels:
#                     first_channel = list(foxglove_channels.values())[0]
#                     await server.send_message(
#                         first_channel,
#                         start_time or 0,
#                         json.dumps({
#                             "op": "status",
#                             "level": "info",
#                             "message": "Playback paused"
#                         }).encode("utf-8")
#                     )
#
#     mcap_file = "/data/input.mcap"
#     if not os.path.exists(mcap_file):
#         print(f"Error: File {mcap_file} not found")
#         return
#
#     try:
#         with open(mcap_file, "rb") as f:
#             reader = make_reader(f)
#             channels_info = {}
#             schemas_info = {}
#             messages = []
#             start_time: Optional[int] = None
#             end_time: Optional[int] = None
#
#             for schema, channel, message in reader.iter_messages():
#                 if schema and schema.id not in schemas_info:
#                     schemas_info[schema.id] = {
#                         "name": schema.name,
#                         "encoding": schema.encoding,
#                         "data": schema.data
#                     }
#                 if channel and channel.id not in channels_info:
#                     channels_info[channel.id] = {
#                         "topic": channel.topic,
#                         "message_encoding": channel.message_encoding,
#                         "schema_id": channel.schema_id,
#                         "metadata": channel.metadata
#                     }
#                 if message:
#                     messages.append((channel.id, message))
#                     if start_time is None or message.log_time < start_time:
#                         start_time = message.log_time
#                     if end_time is None or message.log_time > end_time:
#                         end_time = message.log_time
#
#             if start_time and end_time:
#                 print(f"MCAP time range: {start_time} to {end_time} ns ({(end_time - start_time) / 1e9} seconds)")
#             else:
#                 print("Warning: No valid timestamps found in MCAP")
#
#     except Exception as e:
#         print(f"Error reading MCAP: {str(e)}")
#         return
#
#     async with FoxgloveServerWithCapabilities("0.0.0.0", 8765, "mcap-server") as server:
#         if start_time is not None and end_time is not None:
#             server.set_capabilities(["time", "clientPublish", "playbackControl", "play", "pause", "seek"])
#             server.set_time_range(start_time, end_time)
#         else:
#             print("Warning: Invalid time range, using fallback")
#             server.set_capabilities(["time", "clientPublish", "playbackControl", "play", "pause", "seek"])
#             server.set_time_range(0, 1000000000)
#
#         listener = Listener()
#         server.set_listener(listener)
#
#         foxglove_channels = {}
#         for channel_id, channel in channels_info.items():
#             schema = schemas_info.get(channel["schema_id"])
#             schema_data = ""
#             schema_encoding = "json"
#             if schema:
#                 try:
#                     if schema["encoding"] == "jsonschema":
#                         schema_data = schema["data"].decode("utf-8")
#                         json.loads(schema_data)
#                         schema_encoding = "jsonschema"
#                     elif schema["encoding"] == "protobuf":
#                         schema_data = base64.b64encode(schema["data"]).decode("utf-8")
#                         schema_encoding = "protobuf"
#                     else:
#                         schema_data = base64.b64encode(schema["data"]).decode("utf-8")
#                         schema_encoding = schema["encoding"] or "raw"
#                 except (UnicodeDecodeError, json.JSONDecodeError) as e:
#                     print(f"Schema processing error for {channel['topic']}: {e}")
#                     schema_data = base64.b64encode(schema["data"]).decode("utf-8")
#                     schema_encoding = "raw"
#
#             channel_params = {
#                 "topic": channel["topic"],
#                 "encoding": channel["message_encoding"] or "json",
#                 "schemaName": schema["name"] if schema else channel["topic"],
#                 "schema": schema_data,
#                 "schemaEncoding": schema_encoding,
#             }
#
#             print(f"Registering channel: {channel['topic']} (encoding: {channel_params['encoding']}, schema: {channel_params['schemaName']})")
#             try:
#                 chan_id = await server.add_channel(channel_params)
#                 foxglove_channels[channel_id] = chan_id
#                 print(f"Successfully registered channel {channel['topic']} (ID: {chan_id})")
#             except Exception as e:
#                 print(f"Failed to register channel {channel['topic']}: {str(e)}")
#                 continue
#
#         if foxglove_channels:
#             first_channel = list(foxglove_channels.values())[0]
#             await server.send_message(
#                 first_channel,
#                 start_time or 0,
#                 json.dumps({
#                     "op": "status",
#                     "level": "info",
#                     "message": "Server initialized, ready for playback"
#                 }).encode("utf-8")
#             )
#             print("Sent initial status message to clients")
#
#         if start_time and foxglove_channels:
#             for channel_id, message in messages:
#                 if channel_id in foxglove_channels:
#                     await server.send_message(
#                         foxglove_channels[channel_id],
#                         message.log_time,
#                         message.data
#                     )
#                     print(f"Sent initial message at time {message.log_time}")
#                     break
#
#         def find_message_index(seek_time: int) -> int:
#             for i, (_, message) in enumerate(messages):
#                 if message.log_time >= seek_time:
#                     return i
#             return len(messages) - 1
#
#         total = len(messages)
#         while True:
#             start_index = 0
#             prev_time = None
#             if listener.seek_time is not None:
#                 start_index = find_message_index(listener.seek_time)
#                 print(f"Seeking to index {start_index} for time {listener.seek_time}")
#                 listener.seek_time = None
#
#             for i in range(start_index, len(messages)):
#                 channel_id, message = messages[i]
#                 if channel_id not in foxglove_channels:
#                     continue
#
#                 channel_info = channels_info[channel_id]
#
#                 while not listener.play:
#                     await asyncio.sleep(0.1)
#
#                 if prev_time is not None:
#                     delay = (message.log_time - prev_time) / 1e9
#                     elapsed = 0
#                     while elapsed < delay:
#                         if listener.seek_time is not None or not listener.play:
#                             break
#                         await asyncio.sleep(0.05)
#                         elapsed += 0.05
#
#                     if listener.seek_time is not None or not listener.play:
#                         print("Interrupted by seek or pause")
#                         break
#
#                 try:
#                     message_data = message.data
#                     if channel_info["message_encoding"] == "json":
#                         try:
#                             message_data = message.data.decode("utf-8")
#                             json.loads(message_data)
#                         except (UnicodeDecodeError, json.JSONDecodeError):
#                             message_data = base64.b64encode(message.data).decode("utf-8")
#                     elif channel_info["message_encoding"] == "protobuf":
#                         message_data = message.data
#                     else:
#                         message_data = message.data
#
#                     await server.send_message(
#                         foxglove_channels[channel_id],
#                         message.log_time,
#                         message_data
#                     )
#                     print(f"Sent {i+1}/{total} to {channel_info['topic']} [{len(message.data)} bytes]", end="\r")
#
#                 except Exception as e:
#                     print(f"\nFailed to send message on {channel_info['topic']}: {str(e)}")
#                     continue
#
#                 prev_time = message.log_time
#
#             if listener.seek_time is None:
#                 print("\nPlayback finished")
#                 break
#
#     print("\nServer stopped gracefully")
#
# if __name__ == "__main__":
#     run_cancellable(main())
#
# import asyncio
# import os
# import json
# import base64
# from typing import Optional, List, Tuple, Dict, Any
#
# from mcap.reader import make_reader
# from foxglove_websocket import run_cancellable
# from foxglove_websocket.server import FoxgloveServer, FoxgloveServerListener
# from foxglove_websocket.types import ChannelId
#
# class PlaybackServer(FoxgloveServer):
#     def __init__(self, host: str, port: int, name: str):
#         super().__init__(host, port, name)
#     async def add_connection(self, conn):
#         await super().add_connection(conn)
#         await self.reset_session_id(self.session_id)
#
# async def main():
#     mcap_path = "/data/input.mcap"
#     if not os.path.exists(mcap_path):
#         print(f"Error: {mcap_path} not found")
#         return
#
#     channels_info: Dict[int, dict] = {}
#     schemas_info: Dict[int, dict] = {}
#     msgs: List[Tuple[int, Any]] = []
#     t0 = t1 = None
#
#     with open(mcap_path, "rb") as f:
#         reader = make_reader(f)
#         for schema, channel, message in reader.iter_messages():
#             if schema and schema.id not in schemas_info:
#                 schemas_info[schema.id] = {
#                     "name": schema.name,
#                     "encoding": schema.encoding,
#                     "data": schema.data,
#                 }
#             if channel and channel.id not in channels_info:
#                 channels_info[channel.id] = {
#                     "topic": channel.topic,
#                     "message_encoding": channel.message_encoding,
#                     "schema_id": channel.schema_id,
#                     "metadata": channel.metadata,
#                 }
#             if message:
#                 msgs.append((channel.id, message))
#                 ts = message.log_time
#                 t0 = ts if t0 is None or ts < t0 else t0
#                 t1 = ts if t1 is None or ts > t1 else t1
#
#     if t0 is None or t1 is None:
#         print("No timestamps found in MCAP")
#         return
#     print(f"MCAP time range: {t0} … {t1} ({(t1 - t0)/1e9:.2f}s)")
#
#     async with PlaybackServer("0.0.0.0", 8765, "mcap-server") as server:
#         server.capabilities = [
#             "time",
#             "playbackControl",
#             "play",
#             "pause",
#             "seek",
#             "setSpeed",
#             "clientPublish",
#         ]
#         server.supported_encodings = ["json", "protobuf", "ros1"]
#         server.metadata = {
#             "time": {"start": t0, "end": t1},
#             "dataset": "mcap-local-file",
#         }
#         print("serverInfo: capabilities, supportedEncodings и metadata настроены")
#
#         class Listener(FoxgloveServerListener):
#             def __init__(self):
#                 self.play = True
#                 self.seek_to: Optional[int] = None
#                 self.speed: float = 1.0
#
#             async def on_client_message(self, srv: FoxgloveServer, msg: dict):
#                 op = msg.get("op")
#                 if op == "play":
#                     self.play = True
#                 elif op == "pause":
#                     self.play = False
#                 elif op == "seek":
#                     try:
#                         self.seek_to = int(msg["time"])
#                     except:
#                         pass
#                 elif op == "setSpeed":
#                     try:
#                         self.speed = float(msg.get("speed", self.speed))
#                     except:
#                         pass
#                     print(f"New speed: {self.speed}")
#
#         listener = Listener()
#         server.set_listener(listener)
#
#         fox_ids: Dict[int, ChannelId] = {}
#         for cid, info in channels_info.items():
#             enc = info["message_encoding"]
#             sch = schemas_info.get(info["schema_id"])
#             if enc == "json" and sch and sch["encoding"] == "jsonschema":
#                 schema_data = sch["data"].decode("utf-8")
#                 schema_enc = "jsonschema"
#             elif enc == "protobuf" and sch and sch["encoding"] == "protobuf":
#                 schema_data = base64.b64encode(sch["data"]).decode()
#                 schema_enc = "protobuf"
#             elif enc == "ros1" and sch and sch["encoding"] == "ros1":
#                 schema_data = sch["name"]
#                 schema_enc = "ros1"
#             else:
#                 continue
#
#             params = {
#                 "topic": info["topic"],
#                 "encoding": enc,
#                 "schemaName": sch["name"],
#                 "schema": schema_data,
#                 "schemaEncoding": schema_enc,
#             }
#             fox_id = await server.add_channel(params)
#             fox_ids[cid] = fox_id
#             print(f"Registered channel {cid} → {info['topic']} as {enc}/{schema_enc}")
#
#         if fox_ids:
#             ch0 = next(iter(fox_ids.values()))
#             ready = json.dumps({"op":"status","level":"info","message":"Ready"}).encode()
#             await server.send_message(ch0, t0, ready)
#             await server.broadcast_time(t0)
#
#         total = len(msgs)
#         idx = 0
#         prev_ts: Optional[int] = None
#
#         while True:
#             if listener.seek_to is not None:
#                 tgt = listener.seek_to
#                 for j, (_, m) in enumerate(msgs):
#                     if m.log_time >= tgt:
#                         idx, prev_ts = j, None
#                         break
#                 listener.seek_to = None
#
#             if idx >= total:
#                 await asyncio.sleep(0.1)
#                 continue
#
#             ch_id, m = msgs[idx]
#             if ch_id not in fox_ids:
#                 idx += 1
#                 continue
#
#             while not listener.play:
#                 await asyncio.sleep(0.05)
#
#             data = m.data
#             if channels_info[ch_id]["message_encoding"] == "json":
#                 try:
#                     text = data.decode("utf-8")
#                     json.loads(text)
#                     payload = text.encode("utf-8")
#                 except:
#                     payload = base64.b64encode(data)
#             else:
#                 payload = data
#
#             aid = fox_ids[ch_id]
#             await server.send_message(aid, m.log_time, payload)
#             await server.broadcast_time(m.log_time)
#
#             if prev_ts is not None:
#                 raw_dt = (m.log_time - prev_ts) / 1e9
#                 if "image_rect_compressed" in channels_info[ch_id]["topic"]:
#                     raw_dt = min(raw_dt, 1/30)
#                 dt = raw_dt / listener.speed if listener.speed > 0 else 0
#                 await asyncio.sleep(dt)
#             prev_ts = m.log_time
#             idx += 1
# 
# if __name__ == "__main__":
#     run_cancellable(main())


import asyncio
import os
import json
import base64
from typing import Optional, List, Tuple, Dict, Any

from mcap.reader import make_reader
from foxglove_websocket import run_cancellable
from foxglove_websocket.server import FoxgloveServer, FoxgloveServerListener
from foxglove_websocket.types import ChannelId

class PlaybackServer(FoxgloveServer):
    async def _send_server_info(self, connection):
        await super()._send_server_info(connection)
        start_ts = self.metadata["time"]["start"]  # type: ignore
        await self.broadcast_time(int(start_ts))

async def main():
    mcap_path = "/data/input.mcap"
    if not os.path.exists(mcap_path):
        print(f"Error: {mcap_path} not found")
        return

    channels_info: Dict[int, dict] = {}
    schemas_info: Dict[int, dict] = {}
    msgs: List[Tuple[int, Any]] = []
    t0: Optional[int] = None
    t1: Optional[int] = None

    with open(mcap_path, "rb") as f:
        reader = make_reader(f)
        for schema, channel, message in reader.iter_messages():
            if schema and schema.id not in schemas_info:
                schemas_info[schema.id] = {
                    "name": schema.name,
                    "encoding": schema.encoding,
                    "data": schema.data,
                }
            if channel and channel.id not in channels_info:
                channels_info[channel.id] = {
                    "topic": channel.topic,
                    "message_encoding": channel.message_encoding,
                    "schema_id": channel.schema_id,
                    "metadata": channel.metadata,
                }
            if message:
                msgs.append((channel.id, message))
                ts = message.log_time
                t0 = ts if t0 is None or ts < t0 else t0
                t1 = ts if t1 is None or ts > t1 else t1

    if t0 is None or t1 is None:
        print("No timestamps found in MCAP")
        return
    print(f"MCAP time range: {t0} … {t1} ({(t1 - t0)/1e9:.2f}s)")

    async with PlaybackServer(
                "0.0.0.0",
                8765,
                "mcap-server",
                capabilities=["time", "playbackControl", "clientPublish"],
                supported_encodings=["json", "protobuf", "ros1"],
                metadata={"time": {"start": t0, "end": t1}, "dataset": "mcap-local-file"},
            ) as server:
        print("Server started with playbackControl + time support")



        class Listener(FoxgloveServerListener):
            def __init__(self):
                self.play = True
                self.seek_to: Optional[int] = None
                self.speed: float = 1.0

            async def on_client_message(self, srv: FoxgloveServer, msg: dict):
                op = msg.get("op")
                if op == "play":
                    self.play = True
                elif op == "pause":
                    self.play = False
                elif op == "seek":
                    try:
                        self.seek_to = int(msg["time"])
                    except Exception:
                        pass
                elif op == "setSpeed":
                    try:
                        self.speed = float(msg.get("speed", self.speed))
                    except:
                        pass
                    print(f"New speed: {self.speed}")


        listener = Listener()
        server.set_listener(listener)

        fox_ids: Dict[int, ChannelId] = {}
        for cid, info in channels_info.items():
            enc = info["message_encoding"]
            sch = schemas_info.get(info["schema_id"])
            if enc == "json" and sch and sch["encoding"] == "jsonschema":
                schema_data = sch["data"].decode("utf-8")
                schema_enc = "jsonschema"
            elif enc == "protobuf" and sch and sch["encoding"] == "protobuf":
                schema_data = base64.b64encode(sch["data"]).decode()
                schema_enc = "protobuf"
            elif enc == "ros1" and sch and sch["encoding"] == "ros1":
                schema_data = sch["name"]
                schema_enc = "ros1"
            else:
                continue

            params = {
                "topic": info["topic"],
                "encoding": enc,
                "schemaName": sch["name"],
                "schema": schema_data,
                "schemaEncoding": schema_enc,
            }
            fox_id = await server.add_channel(params)
            fox_ids[cid] = fox_id
            print(f"Registered channel {cid} → {info['topic']} ({enc}/{schema_enc})")

        if fox_ids:
            first = next(iter(fox_ids.values()))
            ready = json.dumps({
                "op": "status",
                "level": "info",
                "message": "Ready",
            }).encode("utf-8")
            await server.send_message(first, t0, ready)

        total = len(msgs)
        idx = 0
        prev_ts: Optional[int] = None

        while True:
            if listener.seek_to is not None:
                tgt = listener.seek_to
                for j, (_, m) in enumerate(msgs):
                    if m.log_time >= tgt:
                        idx, prev_ts = j, None
                        break
                listener.seek_to = None

            if idx >= total:
                await asyncio.sleep(0.1)
                continue

            ch_id, m = msgs[idx]
            if ch_id not in fox_ids:
                idx += 1
                continue

            while not listener.play:
                await asyncio.sleep(0.05)

            data = m.data
            if channels_info[ch_id]["message_encoding"] == "json":
                try:
                    txt = data.decode("utf-8")
                    json.loads(txt)
                    payload = txt.encode("utf-8")
                except Exception:
                    payload = base64.b64encode(data)
            else:
                payload = data

            aid = fox_ids[ch_id]
            await server.send_message(aid, m.log_time, payload)
            await server.broadcast_time(m.log_time)

            if prev_ts is not None:
                dt_real = (m.log_time - prev_ts) / 1e9
                if "image_rect_compressed" in channels_info[ch_id]["topic"]:
                    dt_real = min(dt_real, 1/30)
                await asyncio.sleep(dt_real / max(listener.speed, 1e-6))

            prev_ts = m.log_time
            idx += 1

if __name__ == "__main__":
    run_cancellable(main())
