/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    boxstate.js
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

class BoxState {

    static stateColors = {
        "opt": {
            "bg": "#239b56",
            "fg": "#eafaf1"
        },
        "att": {
            "bg": "#f1c40f",
            "fg": "#000000"
        },
        "exc": {
            "bg": "#c0392b",
            "fg": "#f9ebea"
        },
        "ntr": {
            "bg": "#4d5656",
            "fg": "#eaeded"
        },
        "frz": {
            "bg": "#3498db",
            "fg": "#d6eaf8"
        },
        "lit": {
            "bg": "#e0e0e0",
            "fg": "#303030"
        }
    };

    static setStateColors(selector, colors) {
        let obj;
        if (typeof selector == "object") {
            obj = selector;
        }
        else {
            obj = $(selector);
        }
        obj.css( "background-color", colors["bg"] );
        obj.css( "color", colors["fg"] );
    }

    static optimal(selector) {
        BoxState.setStateColors(selector, BoxState.stateColors["opt"]);
    }

    static attention(selector) {
        BoxState.setStateColors(selector, BoxState.stateColors["att"]);
    }

    static exception(selector) {
        BoxState.setStateColors(selector, BoxState.stateColors["exc"]);
    }

    static neutral(selector) {
        BoxState.setStateColors(selector, BoxState.stateColors["ntr"]);
    }

    static frozen(selector) {
        BoxState.setStateColors(selector, BoxState.stateColors["frz"]);
    }

    static light(selector) {
        BoxState.setStateColors(selector, BoxState.stateColors["lit"]);
    }

}
