/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    clock.js
 * Author:  Stian Lysne <...>
 * 
 * Copyright © 2025 Rakuten, Inc.
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
 * 
 *****************************************************************************/

class Clock {

    constructor() {
        this.refresher = null;
    }


    setElement(elem) {
        this.elem = elem;
    }


    running() {
        return this.refresher != null;
    }


    start(reference) {
        if (this.refresher != null) {
            clearInterval(this.refresher);
        }
        this.updateTimeReference(reference);
        this.refresher = setInterval( this.refresh.bind(this), 77 );
    }


    stop() {
        if (this.refresher != null) {
            clearInterval(this.refresher);
            this.refresher = null;
        }
        if (this.elem != null) {
            this.elem.val(this.secondsToDHMS(null));
        }
    }


    refresh() {
        try {
            if (this.elem != null) {
                const seconds = this.getTime();
                const dhms = this.secondsToDHMS(seconds);
                this.elem.val(dhms);
            }
        }
        catch (error) {
        }
    }


    updateTimeReference(reference) {
        const d = new Date();
        this.tms0 = d.getTime();
        this.reference = reference;
    }


    getTime() {
        const d = new Date();
        const tms1 = d.getTime();
        const seconds = this.reference + (tms1 - this.tms0)/1000.0;
        return seconds;
    }


    secondsToDHMS(seconds) {
        if (seconds != null && seconds > 0) {
            const recent_start = seconds < 60;
            const D = Math.floor( seconds / 86400 );
            seconds = seconds % 86400;
            const H = Math.floor( seconds / 3600 );
            seconds = seconds % 3600;
            const M = Math.floor( seconds / 60 );
            const S = Math.floor(seconds % 60);
            let DHMS;
            if (recent_start) {
                const d = (seconds < 10) ? 2 : 1;
                DHMS = seconds.toFixed(d).padStart(2, "0");
            }
            else {
                DHMS = D.toString().padStart(3, "0") + ":" + H.toString().padStart(2, "0") + ":" + M.toString().padStart(2, "0") + ":" + S.toString().padStart(2, "0");
            }
            return DHMS;
        }
        else {
            return "---:--:--:--";
        }
    }
}
