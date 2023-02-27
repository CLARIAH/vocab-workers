import stomp
import logging

log = logging.getLogger(__name__)


class Listener(stomp.ConnectionListener):
    def __init__(self, conn, workers):
        self._conn = conn
        self._workers = workers

    def on_error(self, frame):
        log.error(frame.body)

    def on_message(self, frame):
        try:
            log.info('Received a task with id "%s"' % frame.body)

            for worker in self._workers:
                worker.run(frame.body)

            log.info('Processed a task with id "%s"' % frame.body)
            self._conn.ack(id=frame.headers['message-id'], subscription=frame.headers['subscription'])
        except Exception as e:
            log.error(e)

    def on_disconnected(self):
        log.info('Disconnecting...')
