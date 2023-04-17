import pika


class Rabbit:
    """
    This class for working with RabbitMQ:
    * connection with device
    * create queue
    * filling and emptying the queue
    * messaging of events
    * logging of internal processes
    * close queue
    """
    host = 'localhost'
    channel = None
    queue = []

    @staticmethod
    def connect(self, host) -> None:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host))
        channel = connection.channel()

        self.channel = channel

    @staticmethod
    def queue_create(self, queue_name: str) -> None:
        self.queue.append(queue_name)
        self.channel.queue_declare(queue=queue_name, durable=True)
        # channel.queue_declare(queue='info')
        # channel.queue_declare(queue='elastic')
        # channel.queue_declare(queue='logger')

    @staticmethod
    def messaging(self, queue_name: str, m: str) -> None:
        message = m

        self.channel.basic_publish(exchange='',
                                   routing_key=queue_name,
                                   body=message,
                                   properties=pika.BasicProperties(
                                       delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                                   ))

        self.channel.exchange_declare(exchange='logs',
                                      exchange_type='fanout')

    @staticmethod
    def logs_file(log) -> None:
        log.dir = "logs/rabbitmq"
        log.file = "rabbit.log"

        # use microseconds since UNIX epoch for timestamp format
        log.file.formatter.time_format = "epoch_usecs"

        # # rotate every day at 23:00 (11:00 p.m.)
        # log.file.rotation.date = "$D23"

        # rotate when the file reaches 10 MiB
        log.file.rotation.size = 10485760

        # keep up to 5 archived log files in addition to the current one
        log.file.rotation.count = 5

    @staticmethod
    def logs_console(log) -> None:
        # use microseconds since UNIX epoch for timestamp format
        log.console.formatter.time_format = "epoch_usecs"

        # # rotate every day at 23:00 (11:00 p.m.)
        # log.console.rotation.date = "$D23"

        # rotate when the file reaches 10 MiB
        log.console.rotation.size = 10485760

        # keep up to 5 archived log files in addition to the current one
        log.console.rotation.count = 5


class Log:
    """
    This class for info of statements my code
    not in RabbitMQ structure
    """
    pass


class Thread:
    """This class for """
    pass

