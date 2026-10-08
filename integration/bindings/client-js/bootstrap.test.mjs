// Finite protocol-5 Bootstrap through the native runtime.
import test from 'node:test';
import {Transaction} from '../../../packages/client-js/transaction.mts';
import {bootstrapSuite} from './bootstrap-harness.mjs';
bootstrapSuite(test,Transaction);
