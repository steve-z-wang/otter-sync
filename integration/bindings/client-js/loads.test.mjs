// Load manager is retired; Query once/refresh has its own native fixtures.
import test from 'node:test';
import {Transaction} from '../../../packages/client-js/transaction.mts';
import {bootstrapSuite} from './bootstrap-harness.mjs';
bootstrapSuite(test,Transaction);
