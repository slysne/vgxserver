/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    digest.js
 * Author:  Stian Lysne slysne.dev@gmail.com
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

class Digest {

    currentDigest = "00000000000000000000000000000000";

    dsym = [
        "&#x259A",
        "&#x259E",
        "&#x2599",
        "&#x259B",
        "&#x259C",
        "&#x259F",
        "&#x2597",
        "&#x2599"
    ];

    nosym = "&#x2589;";

    constructor( elem ) {
        this.elem = elem;
        this.elem.html(
          '<span class="digestBox">' +
            '<span class="digestX digestA"></span>' +
            '<span class="digestX digestB"></span>' +
            '<span class="digestX digestC"></span>' +
            '<span class="digestX digestD"></span>' +
            '<span class="digestX digestE"></span>' +
          '</span>'
        );
        this.update(null);
        $('.digestBox').css("position", "relative");
        $('.digestBox').css("display", "inline-block");
        $('.digestBox').css("font-family", "Consolas, 'Apple Symbols', sans-serif");
        $('.digestX').css("padding-right", "1px");

    }


    update(digest) {
        let X = this.elem.find('.digestX');

        if (digest == null) {
            X.html(this.nosym);
            X.css("visibility","hidden");
            return;
        }
        else if (typeof digest == "string" && digest.length == 32) {
            this.currentDigest = digest;
        }
        else {
            return;
        }

        try {
            const quant = 6;
            const dull = 1;
            const scale = (1<<quant)-dull;
            const dig1 = Math.round(parseInt(this.currentDigest.slice( 0,  2), 16) >> quant) * scale;
            const dig2 = Math.round(parseInt(this.currentDigest.slice( 2,  4), 16) >> quant) * scale;
            const dig3 = Math.round(parseInt(this.currentDigest.slice( 4,  6), 16) >> quant) * scale;
            const dig4 = Math.round(parseInt(this.currentDigest.slice( 6,  8), 16) >> quant) * scale;
            const dig5 = Math.round(parseInt(this.currentDigest.slice( 8, 10), 16) >> quant) * scale;
            const dig6 = Math.round(parseInt(this.currentDigest.slice(10, 12), 16) >> quant) * scale;
            const dig7 = Math.round(parseInt(this.currentDigest.slice(12, 14), 16) >> quant) * scale;
            const dig8 = Math.round(parseInt(this.currentDigest.slice(14, 16), 16) >> quant) * scale;
            const dig9 = Math.round(parseInt(this.currentDigest.slice(16, 18), 16) >> quant) * scale;
            const digA = Math.round(parseInt(this.currentDigest.slice(18, 20), 16) >> quant) * scale;
            const digB = Math.round(parseInt(this.currentDigest.slice(20, 22), 16) >> quant) * scale;
            const digC = Math.round(parseInt(this.currentDigest.slice(22, 24), 16) >> quant) * scale;
            const digD = Math.round(parseInt(this.currentDigest.slice(24, 26), 16) >> quant) * scale;
            const digE = Math.round(parseInt(this.currentDigest.slice(26, 28), 16) >> quant) * scale;
            const digF = Math.round(parseInt(this.currentDigest.slice(28, 30), 16) >> quant) * scale;

            const symA = parseInt(this.currentDigest.slice(0,  2), 16) % 8;
            const symB = parseInt(this.currentDigest.slice(2,  4), 16) % 8;
            const symC = parseInt(this.currentDigest.slice(4,  6), 16) % 8;
            const symD = parseInt(this.currentDigest.slice(6,  8), 16) % 8;
            const symE = parseInt(this.currentDigest.slice(8, 10), 16) % 8;

            let digestA = this.elem.find('.digestA');
            let digestB = this.elem.find('.digestB');
            let digestC = this.elem.find('.digestC');
            let digestD = this.elem.find('.digestD');
            let digestE = this.elem.find('.digestE');

            digestA.css("color", "rgb(" + (dig1) + "," + (dig6) + "," + (digB) + ")");
            digestB.css("color", "rgb(" + (dig2) + "," + (dig7) + "," + (digC) + ")");
            digestC.css("color", "rgb(" + (dig3) + "," + (dig8) + "," + (digD) + ")");
            digestD.css("color", "rgb(" + (dig4) + "," + (dig9) + "," + (digE) + ")");
            digestE.css("color", "rgb(" + (dig5) + "," + (digA) + "," + (digF) + ")");

            digestA.html(this.dsym[symA]);
            digestB.html(this.dsym[symB]);
            digestC.html(this.dsym[symC]);
            digestD.html(this.dsym[symD]);
            digestE.html(this.dsym[symE]);

            X.css("visibility","visible");
        }
        catch(error) { /* ignore */ }
    }
}


class MasterSerial {
    static render( serial ) {
        let msA = 0;
        let msB = 0;
        let msC = 0;
        let msD = 0;
        if (serial != undefined) {
            try {
                const ms = BigInt(serial);
                const bf = BigInt(0x100000000);
                const msLo = Number(ms % bf);
                const msHi = Number(ms / bf);
                msA = msLo & 0xFFFF;
                msB = (msLo >> 16) & 0xFFFF;
                msC = msHi & 0xFFFF;
                msD = (msHi >> 16) & 0xFFFF;
            }
            catch (error) {
                // ignore
            }
        }
        const s_msA = msA.toString().padStart(5,'0');
        const s_msB = msB.toString().padStart(5,'0');
        const s_msC = msC.toString().padStart(5,'0');
        const s_msD = msD.toString();
        return s_msD + "-" + s_msC + "-" + s_msB + "-" + s_msA;
    }

    static diff(serial1, serial2) {
        if (serial1 != null && serial2 != null) {
            let d = 0;
            try {
                const s1 = BigInt(serial1);
                const s2 = BigInt(serial2);
                d = Number(s1 - s2);
            }
            catch (error) {
                // ignore
            }
            return Math.abs(d);
        }
        else {
            return 0;
        }
    }
}
