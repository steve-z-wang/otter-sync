// Finite protocol-5 Bootstrap through the native runtime.
import test from 'node:test';
import {Transaction} from '../../../packages/frontend/client-js/api/transaction.mts';
import {bootstrapSuite} from './bootstrap-harness.mjs';
bootstrapSuite(test,Transaction);
