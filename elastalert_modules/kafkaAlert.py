import json
import logging
from elastalert.alerts import Alerter
from confluent_kafka import Producer, KafkaError

kafka_logger = logging.getLogger('kafka_logger')
kafka_logger.setLevel(logging.DEBUG)


class KafkaAlerter(Alerter):
  kafka_logger.info("[ElastAlert:Plugin:Kafka] init")
  """ Push a message to Kafka topic """
  required_options = frozenset([
    'kafka_brokers',
    'kafka_ca_location',
    'kafka_pub_location',
    'kafka_priv_location',
    'kafka_priv_pass',
    'kafka_groupID',
    'kafka_topic',
    'kafka_security_protocol'
  ])

  def __init__(self, rule):
    super(KafkaAlerter, self).__init__(rule)

    self.KAFKA_TOPIC = self.rule['kafka_topic']
    self.kafka_GROUPID = self.rule['kafka_groupID'] if self.rule.get('kafka_groupID', None) else 'elastalert'
    self.KAFKA_CONFIG = {
      'bootstrap.servers': self.rule['kafka_brokers'],
      'security.protocol': self.rule['kafka_security_protocol'],
      'ssl.ca.location': self.rule['kafka_ca_location'],
      'ssl.certificate.location': self.rule['kafka_pub_location'],
      'ssl.key.location' : self.rule['kafka_priv_location'],
      'ssl.keystore.password' : self.rule['kafka_priv_pass'],
      'group.id': self.kafka_GROUPID,

      'default.topic.config': {
        'auto.offset.reset': 'earliest'
      }
    }
    try:
      kafka_logger.debug("[ElastAlert:Plugin:Kafka:Init] Begin: create Kafka Producer")
      self.kafkaInstance = Producer(self.KAFKA_CONFIG)
      kafka_logger.debug("[ElastAlert:Plugin:Kafka:Init] End: create Kafka Producer, self.kafkaInstance = %s", self.kafkaInstance)
    except Exception as e:
      kafka_logger.exception("[ElastAlert:Plugin:Kafka:Init] Error init kafkaInstance: %s", e)

  def delivery_report(self, err, msg):
    """ Called once for each message produced to indicate delivery result.
      Triggered by poll() or flush(). """
    kafka_logger.debug("[ElastAlert:Plugin:Kafka:Delivery_report] start")
    if err is not None: # Not breaking
      kafka_logger.debug('[*] Message Delivery Error: {}'.format(err))
      kafka_logger.debug('Message Delivery: {}'.format(msg))

  def alert(self, matches):
    try:
      kafka_logger.info("[ElastAlert:Plugin:Kafka:Alert] start")
      body = self.create_alert_body(matches)
      kafka_logger.debug("[ElastAlert:Plugin:Kafka:Alert] body = %s", body)
      if isinstance(body, dict) or isinstance(body, list):
        body = json.dumps(body)

      self.kafkaInstance.poll(0)
      kafka_logger.debug("[ElastAlert:Plugin:Kafka:Alert] self.KAFKA_TOPIC = %s", self.KAFKA_TOPIC)
      self.kafkaInstance.produce(self.KAFKA_TOPIC, body, callback=self.delivery_report)
      self.kafkaInstance.flush()
      kafka_logger.info("[ElastAlert:Plugin:Kafka:Alert] end");
    except Exception as e:
      kafka_logger.error("[ElastAlert:Plugin:Kafka:Alert:ERROR] error = %s", e)

  def get_info(self):
    return {
      'type': 'kafka',
      'config': self.KAFKA_CONFIG,
      'groupID': self.kafka_GROUPID,
      'topic': self.KAFKA_TOPIC,
    }
