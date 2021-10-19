import datetime
from pyais import FileReaderStream
# pip install libais
import ais.stream

# original py ais decoder
start_time = datetime.datetime.now()
counter = 0
for msg in FileReaderStream("12-45.txt"):
    counter += 1
    decoded_message = msg.decode()
    ais_content = decoded_message.content
print(ais_content)
print(counter)
counter = 0
for msg in FileReaderStream("12-46.txt"):
    decoded_message = msg.decode()
    ais_content = decoded_message.content
    counter += 1
print(ais_content)
print(counter)
counter = 0
for msg in FileReaderStream("12-47.txt"):
    decoded_message = msg.decode()
    ais_content = decoded_message.content
    counter += 1
print(ais_content)
print(counter)
counter = 0
end_time = datetime.datetime.now()
print('pyais:',end_time - start_time)

# # ais.stream
start_time = datetime.datetime.now()
with open("12-45.txt") as f:
    for msg in ais.stream.decode(f):
        counter += 1
        ais_content = msg
print(ais_content)
print(counter)
counter = 0
with open("12-46.txt") as f:
    for msg in ais.stream.decode(f):
        ais_content = msg
        counter += 1
print(ais_content)
print(counter)
counter = 0
with open("12-47.txt") as f:
    for msg in ais.stream.decode(f):
        ais_content = msg
        counter += 1
print(ais_content)
print(counter)
end_time = datetime.datetime.now()
print('libais:',end_time - start_time)

# libais is 3x sneller hier, pyais: 14 sec vs libais 4sec
# maar waarom worden er 2000 minder berichten door libais gelezen?
# pyais leest berichten niet met nummers er voor, maar libais wel, zie laatse bericht
# 12-47. Dan zou je juist verwachten dat er meer berichten worden gelezen.