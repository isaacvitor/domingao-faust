
import logging
from random import randint
import datetime

import faust
from faust import Record

logger = logging.getLogger(__name__)

# 01 - Creating a Faust App
app = faust.App("domingao_faust", broker="kafka://localhost:9092", store='memory://')

# 03 - Creating a model
class HotDogJob(Record):
    health: bool
    id: str = None
    create_at: str = None
    bread_type: str = None
    sausage_type: str = None
    style: str = None
    complements: list = None

# 02 - Creating a topic
input_topic = app.topic("df-input-topic", value_type=HotDogJob)
output_topic = app.topic("df-output-topic", value_type=HotDogJob)

# 06 - Creating channels
bread_channel = app.channel()
sausage_channel = app.channel()
style_channel = app.channel()

# 04 - Creating agents
@app.agent(input_topic)
async def health_agent(jobs: HotDogJob):
    async for message in jobs:
        logger.info(message)
        message.id = faust.uuid()
        message.create_at = str(datetime.datetime.now())
        await bread_channel.send(value=message)


@app.agent(bread_channel)
async def bread_agent(jobs: HotDogJob):
    async for message in jobs:
        logger.info(message)
        message.bread_type = 'Whole' if message.health else 'Milk'
        await sausage_channel.send(value=message)

@app.agent(sausage_channel)
async def sausage_agent(jobs: HotDogJob):
    async for message in jobs:
        logger.info(message)
        message.sausage_type = 'Vegan' if message.health else 'Pork'
        await style_channel.send(value=message)

@app.agent(style_channel)
async def style_agent(jobs: HotDogJob):
    async for message in jobs:
        logger.info(message)
        if message.health: 
            message.style = 'American'
        else: 
            message.style = 'Brazilian'
            message.complements = ['Cheddar', 'Ketchup', 'Mustard', 'Corn', 'Onions', 'Tomatoes']
        
        await output_topic.send(value=message)


# 07 - Creating table
output_table = app.Table("df-output-table", default=HotDogJob, partitions=1)

@app.agent(output_topic)
async def output_agent(jobs):
    async for message in jobs:
        logger.info(f'OUTPUT {message}')
        message.finish_at = str(datetime.datetime.now())
        output_table[message.id] = message

# 08 - Creating a web views
@app.page('/jobs/{id}')
@app.table_route(table=output_table, match_info='id')
async def get_hot_dog(web, request, id):
    return web.json({
        'hot_dog': output_table[id]
    })

# 05 - Running Timers
@app.timer(interval=5)
async def send_job():
    type_health = True if randint(0, 1) == 0 else False
    await input_topic.send(value=HotDogJob(health=type_health))

# 01 - Creating a Faust App
if __name__ == '__main__':
    app.main()