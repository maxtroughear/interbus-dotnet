import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Transport-agnostic',
    description: (
      <>
        Write your consumers and publishers once. Swap between in-memory,
        Amazon SNS/SQS, Azure Service Bus, RabbitMQ, or PostgreSQL without
        changing application code.
      </>
    ),
  },
  {
    title: 'Flexible middleware pipeline',
    description: (
      <>
        Compose retry, dead-letter, outbox, and custom middleware around every
        consumer. Mix and match built-in behaviours or plug in your own — each
        consumer's pipeline is fully independent.
      </>
    ),
  },
  {
    title: 'Sagas and scheduling',
    description: (
      <>
        Model stateful workflows with the saga abstraction. Schedule messages
        with TickerQ or Amazon EventBridge — all through a single unified API.
      </>
    ),
  },
];

function Feature({title, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
