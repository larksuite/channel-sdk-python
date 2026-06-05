from lark_channel.core.model import Config

from .resource import Chat, File, Image, Message, MessageReaction, MessageResource


class V1(object):
    def __init__(self, config: Config) -> None:
        self.chat: Chat = Chat(config)
        self.file: File = File(config)
        self.image: Image = Image(config)
        self.message: Message = Message(config)
        self.message_reaction: MessageReaction = MessageReaction(config)
        self.message_resource: MessageResource = MessageResource(config)
