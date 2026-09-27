#!/usr/bin/env python3
import base64, http.client, json, socket, struct, sys
sock_path, token, cmd = sys.argv[1], sys.argv[2], sys.argv[3]
class UDS(http.client.HTTPConnection):
    def __init__(self): super().__init__("envd")
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(sock_path)
req={"process":{"cmd":"/bin/sh","args":["-c",cmd]}}
body=json.dumps(req).encode(); env=b"\x00"+struct.pack(">I",len(body))+body
c=UDS(); c.request("POST","/process.Process/Start",body=env,headers={"Content-Type":"application/connect+json","Connect-Protocol-Version":"1","X-Access-Token":token})
r=c.getresponse(); data=r.read(); out=b""; exit_code=None; err=None; i=0
while i+5<=len(data):
    flag=data[i]; ln=struct.unpack(">I",data[i+1:i+5])[0]; msg=data[i+5:i+5+ln]; i+=5+ln
    j=json.loads(msg) if msg else {}
    if flag&2:
        if j.get("error"): err=j
        continue
    ev=j.get("event",{})
    if "data" in ev:
        for k in ("stdout","stderr"):
            if ev["data"].get(k): out+=base64.b64decode(ev["data"][k])
    if "end" in ev: exit_code=ev["end"].get("exitCode",0)
print("HTTP_STATUS",r.status); print("EXIT_CODE",exit_code)
if err is not None: print("API_ERROR",json.dumps(err))
sys.stdout.write("OUTPUT_BEGIN\n"); sys.stdout.flush(); sys.stdout.buffer.write(out); sys.stdout.write("\nOUTPUT_END\n")
