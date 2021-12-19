from pydantic import BaseModel
from uuid import UUID, uuid4
from typing import Optional, List

class Tweet(BaseModel):
    id: Optional[UUID] = uuid4()
    timestamp: str
    content: str
    likes: int
    retweets: int
    owner: UUID

class User(BaseModel):
    id: Optional[UUID] = uuid4()
    username: str
    password: str

class Message(BaseModel):
    id: Optional[UUID] = uuid4()
    timestamp: str
    content: str
    owner: UUID
    conversation: UUID

class Conversation(BaseModel):
    id: Optional[UUID] = uuid4()
    participants: List[User]
    messages: List[Message]
