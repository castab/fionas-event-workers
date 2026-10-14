import { parseArgs } from 'node:util';
import { connect } from '@nats-io/transport-node';
import { jetstreamManager } from '@nats-io/jetstream';
import { connectionOptions, natsConfig } from '../workers/inquiry-to-commerce/src/config.ts';
import { provision } from '../workers/inquiry-to-commerce/src/provision.ts';

try {
  const { values } = parseArgs({ options: {
    'retention-days': { type: 'string', default: '30' },
    replicas: { type: 'string', default: '1' },
  } });
  const nc = await connect(await connectionOptions(natsConfig(process.env), 'fionas-workers-setup'));
  try {
    // Skip the general account INFO query; operator rights are used only for explicit resources.
    await provision(await jetstreamManager(nc, { checkAPI: false }), Number(values['retention-days']), Number(values.replicas));
    console.log('FIONAS_EVENTS and inquiry-to-commerce verified/provisioned; FIONAS_INQUIRIES unchanged.');
  } catch (error) {
    if (error instanceof Error && /configuration mismatch|must already exist|Retention must|Replicas must/.test(error.message)) console.error(error.message);
    else console.error('Provisioning failed; check operator credentials, NATS availability, and the existing input stream.');
    process.exitCode = 1;
  } finally { await nc.close(); }
} catch {
  console.error('Setup failed; check NATS_URL, authentication and --retention-days/--replicas options.');
  process.exitCode = 1;
}
