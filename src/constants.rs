/*
 * Copyright (c) 2015, Hewlett Packard Development Company L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
pub const CAN_DO:             u32 = 1;
pub const CANT_DO:            u32 = 2;
pub const RESET_ABILITIES:    u32 = 3;
pub const PRE_SLEEP:          u32 = 4;
// 5 is unused
pub const NOOP:               u32 = 6;
pub const SUBMIT_JOB:         u32 = 7;
pub const JOB_CREATED:        u32 = 8;
pub const GRAB_JOB:           u32 = 9;
pub const NO_JOB:             u32 = 10;
pub const JOB_ASSIGN:         u32 = 11;
pub const WORK_STATUS:        u32 = 12;
pub const WORK_COMPLETE:      u32 = 13;
pub const WORK_FAIL:          u32 = 14;
pub const GET_STATUS:         u32 = 15;
pub const ECHO_REQ:           u32 = 16;
pub const ECHO_RES:           u32 = 17;
pub const SUBMIT_JOB_BG:      u32 = 18;
pub const ERROR:              u32 = 19;
pub const STATUS_RES:         u32 = 20;
pub const SUBMIT_JOB_HIGH:    u32 = 21;
pub const SET_CLIENT_ID:      u32 = 22;
pub const CAN_DO_TIMEOUT:     u32 = 23;
pub const ALL_YOURS:          u32 = 24;
pub const WORK_EXCEPTION:     u32 = 25;
pub const OPTION_REQ:         u32 = 26;
pub const OPTION_RES:         u32 = 27;
pub const WORK_DATA:          u32 = 28;
pub const WORK_WARNING:       u32 = 29;
pub const GRAB_JOB_UNIQ:      u32 = 30;
pub const JOB_ASSIGN_UNIQ:    u32 = 31;
pub const SUBMIT_JOB_HIGH_BG: u32 = 32;
pub const SUBMIT_JOB_LOW:     u32 = 33;
pub const SUBMIT_JOB_LOW_BG:  u32 = 34;
pub const SUBMIT_JOB_SCHED:   u32 = 35;
pub const SUBMIT_JOB_EPOCH:   u32 = 36;

pub const REQ: [u8;4] = [0x00u8, 'R' as u8, 'E' as u8, 'Q' as u8];
pub const RES: [u8;4] = [0x00u8, 'R' as u8, 'E' as u8, 'S' as u8];

#[derive(PartialEq)]
pub enum PacketCode {
    REQ,
    RES,
}
