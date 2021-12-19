from typing import Optional, List

from fastapi import Security, Depends, FastAPI, HTTPException
from fastapi.security.api_key import APIKey

from starlette import status
from starlette.responses import RedirectResponse, JSONResponse

from models import Tweet, User, Conversation, Message
from api_keys import get_api_key

from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('twitter')

session.execute("CREATE TABLE IF NOT EXISTS twitter.tweet(\n" +
        "        id uuid,\n" +
        "        timestamp text,\n" +
        "        content text,\n" +
        "        likes int,\n" +
        "        retweets int,\n" +
        "        owner uuid,\n" +
        "        PRIMARY KEY (id)\n" +
        "    )");

session.execute("CREATE TABLE IF NOT EXISTS twitter.user(\n" +
        "        id uuid,\n" +
        "        username text,\n" +
        "        password text,\n" +
        "        PRIMARY KEY (id)\n" +
        "    )");

session.execute("CREATE TABLE IF NOT EXISTS twitter.message(\n" +
        "        id uuid,\n" +
        "        timestamp timestamp,\n" +
        "        content text,\n" +
        "        owner uuid,\n" +
        "        conversation uuid,\n" +
        "        PRIMARY KEY (id)\n" +
        "    )");

session.execute("CREATE TABLE IF NOT EXISTS twitter.conversation(\n" +
        "        id uuid,\n" +
        "        participants list<uuid>,\n" +
        "        messages list<uuid>,\n" +
        "        PRIMARY KEY (id)\n" +
        "    )");

app = FastAPI()

@app.get("/health")
async def health():
    """
    Make sur your API is running
    """
    return JSONResponse(
        {"detail": "All good!"},
        status_code=status.HTTP_200_OK
    )

@app.get("/health-secure")
async def health_secured(api_key: APIKey = Depends(get_api_key)):
    """
    Make sure your API credentials are valid and run
    """
    return JSONResponse(
        {"detail": "All good!"},
        status_code=status.HTTP_200_OK
    )

@app.get("/tweet/{id}", tags=["Tweet Service"])
async def get_single_tweet(id:str, api_key: APIKey = Depends(get_api_key)):
    """
    Return a specific tweet
    """
    rows = session.execute("SELECT * FROM Tweet WHERE id = {};", [id])
    data = {'items': {}}
    for row in rows:
        data['items'][str(row.id)] = {
            'id': str(row.id),
            'timestamp': str(row.timestamp),
            'content': str(row.content),
            'likes': row.likes,
            'retweets': row.retweets,
            'owner': str(row.owner)
        }
    data['count'] = len(data['items'])
    return JSONResponse(
        data,
        status_code=status.HTTP_200_OK
    )

@app.post("/tweet/new", tags=["Tweet Service"])
async def post_tweet(tweet: Tweet,
               api_key: APIKey = Depends(get_api_key)):
    """
    Add a new tweet in the table
    """
    returned_value = session.execute(
        "INSERT INTO Tweet (id, timestamp, content, likes, retweets, owner) VALUES ({}, '{}', '{}', {}, {}, {});".format(tweet.id, tweet.timestamp, tweet.content, tweet.likes, tweet.retweets, tweet.owner)
    )
    return JSONResponse(
        {'detail': "tweet has been added"},
        status_code=status.HTTP_200_OK
    )

@app.put("/tweet", tags=["Tweet Service"])
async def put_tweet(item: Tweet,
              api_key: APIKey = Depends(get_api_key)):
    """
    Update tweet in the table
    """
    returned_value = session.execute(
        """
        UPDATE Tweet
        SET content = '{}', likes = {}, retweets = {}
        WHERE id = {};
        """.format(item.content, item.likes, item.retweets, item.id)
    )
    return JSONResponse(
        {'detail': "tweet has been updated"},
        status_code=status.HTTP_200_OK
    )

# /tweet/{table}?id=xxxxxxxxxx
@app.delete("/tweet", tags=["Tweet Service"])
async def delete_tweet(id: str,
                 api_key: APIKey = Depends(get_api_key)):
    """
    Delete tweet from the table
    """
    returned_value = session.execute(
        """
        DELETE FROM Tweet WHERE id = {};
        """.format(id)
    )
    # can be --> WHERE id IN (xxxxxxxxxxxxxxx, xxxxxxxxxxxxxxx);
    return JSONResponse(
        {'detail': "tweet has been deleted"},
        status_code=status.HTTP_200_OK
    )

def generate_insert_cql(tweet: Tweet):
    return  "INSERT INTO Tweet (id, timestamp, content, likes, retweets, owner) VALUES ({}, '{}', '{}', {}, {}, {});".format(tweet.id, tweet.timestamp, tweet.content, tweet.likes, tweet.retweets, tweet.owner)

@app.post("/tweet", tags=["Tweet Service"])
async def batch_post_tweet(items: List[Tweet],
                     api_key: APIKey = Depends(get_api_key)):
    """
    Add many new tweets in the table
    """
    cmd = ""
    for item in items:
        cmd += generate_insert_cql(item)
    returned_value = session.execute(
        """
        BEGIN BATCH
            {}
        APPLY BATCH;
        """.format(cmd)
    )
    return JSONResponse(
        {'detail': "tweets have been added"},
        status_code=status.HTTP_200_OK
    )


@app.post("/search", tags=["Search Service"])
async def search_tweet(search_string: str,
               api_key: APIKey = Depends(get_api_key)):
    """
    Search for a tweet in the table
    """
    rows = session.execute("SELECT * FROM Tweet WHERE content LIKE '%{}%';", search_string)
    data = {'items': {}}
    for row in rows:
        data['items'][str(row.id)] = {
            'id': str(row.id),
            'timestamp': str(row.timestamp),
            'content': str(row.content),
            'likes': row.likes,
            'retweets': row.retweets,
            'owner': str(row.owner)
        }
    data['count'] = len(data['items'])
    return JSONResponse(
        data,
        status_code=status.HTTP_200_OK
    )

@app.post("/user/tweets", tags=["User TimeLine Service"])
async def get_user_tweets(user: str, api_key: APIKey = Depends(get_api_key)):
    """
    Return all user tweets
    """
    rows = session.execute("SELECT * FROM Tweet WHERE owner = {};", user)
    data = {'items': {}}
    for row in rows:
        data['items'][str(row.id)] = {
            'id': str(row.id),
            'timestamp': str(row.timestamp),
            'content': str(row.content),
            'likes': row.likes,
            'retweets': row.retweets,
            'owner': str(row.owner)
        }
    data['count'] = len(data['items'])
    return JSONResponse(
        data,
        status_code=status.HTTP_200_OK
    )

@app.get("/tweet", tags=["Home TimeLine Service"])
async def get_tweets(api_key: APIKey = Depends(get_api_key)):
    """
    Return all tweets
    """
    rows = session.execute("SELECT * FROM Tweet;")
    data = {'items': {}}
    for row in rows:
        data['items'][str(row.id)] = {
            'id': str(row.id),
            'timestamp': str(row.timestamp),
            'content': str(row.content),
            'likes': row.likes,
            'retweets': row.retweets,
            'owner': str(row.owner)
        }
    data['count'] = len(data['items'])
    return JSONResponse(
        data,
        status_code=status.HTTP_200_OK
    )