/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    header.js
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

class CommonHeader {

    port_offset = 0;
    baseport = -1;

    static copySelectionToClipboard(event) {
        if (navigator.clipboard) {
            let selection = document.getSelection();
            let text = selection.toString();
            let args = event.data;
            if (args && args.asint === true) {
                text = text.replace(/,/g, "");
            }
            navigator.clipboard.writeText(text).then(
                () => {
                    selection.empty();
                }
            );
        }
    }

    static ready( callback=null, lowestAcceptableExecutor=15, isAdmin=true, pageLabel=null, protectBasePort=true ) {
        
        if( isAdmin ) {
          $.ajaxSetup({
              timeout: 60000,
              headers: {
                  /*'X-Vgx-Builtin-Min-Executor': "" + lowestAcceptableExecutor*/
              }
          });
        }
        else {
          $.ajaxSetup({
              timeout: 60000,
          });
        }

        $("#commonHeaderDiv").load( "header.html", function () {
            $('#logoimg').click(CommonHeader.leave);
            if( !isAdmin ) {
                $("#commonHeaderTable").find(".admininfo").css( "display", "none" );
                if( pageLabel ) {
                    $(".customLabel").css( "display", "inline" );
                    $("#pageLabel").html( pageLabel );
                    $("#pageLabel").css( "font-weight", "bold" );
                    $("#pageLabel").css( "vertical-align", "middle" );
                    $("#logoimg").css( "vertical-align", "middle" );
                }
            }
            $.get( "vgx/ping", function (data, textStatus, jqXHR) {
                const host = data["response"]["host"];
                const ip = host["ip"];
                const name = host["name"];
                const memory_MiB = Math.round(parseInt(host["memory"]) / (1024 * 1024));
                const port = data["port"][0];
                const port_offset = data["port"][1];
                CommonHeader.port_offset = port_offset;
                CommonHeader.baseport = port - port_offset;
                const rexR = /\(R\)/ig;
                const rexTM = /\(TM\)/ig;
                let cpu = host["cpu"];
                cpu = cpu.replaceAll(rexR, "&reg;").replaceAll(rexTM, "&trade;");
                $('#commonCPU').html(cpu);
                $('#commonMemoryMiB').html(memory_MiB.toLocaleString("en-US"));
                $('#commonIP').html(ip);
                $('#commonHostName').html(name);
                $('#commonAdmin').html("" + port + window.location.pathname);
                $('#commonPort').html("" + (port-port_offset));
                $("#commonFooterDiv").load( "footer.html", function () {
                    if (callback != null) {
                        callback();
                    }
                    $("body").css("display", "block");
                    setTimeout(function () { $("body").addClass("ready"); }, 50);
                });
                $('#commonHostName').mouseup( CommonHeader.copySelectionToClipboard );
                $('#commonIP').mouseup( CommonHeader.copySelectionToClipboard );
                let barInterval = null;
                $('#commonPort').mouseup( function() {
                    if (barInterval != null) {
                        clearInterval( barInterval );
                        barInterval = null;
                        $('#commonPort').html("" + port);
                    }
                } );
                if (isAdmin && port_offset == 0 && protectBasePort) {
                    $('#commonHeaderTable').css("background-color", "#f1948a");
                    $('#commonHeaderTable').attr('title', 'Main Server Port!\n\nClick the bars if you want to remain here.');
                    let barElem = $('#commonPort');
                    let nBar = 20;
                    barInterval = setInterval( function() {
                        nBar -= 1;
                        let bar = "|".repeat( nBar );
                        barElem.html( bar );
                        if( nBar == 0 ) {
                            clearInterval( barInterval );
                            window.location = "http://" + window.location.hostname + ":" + (port + 1) + window.location.pathname;
                        }
                    }, 500 );
                }
            });
        });
    }

    static leave() {
        const path_segments = window.location.pathname.split("/");
        if (path_segments.length < 3) {
            $('body').css("transition", "opacity 150ms ease-in, transform 80ms ease-in");
            $('body').addClass("leavepage");
            setTimeout(function () {
                setTimeout(function () {
                    $('body').removeClass("leavepage");
                }, 1000);
                window.open("/", "_self");
            }, 500);
        }
        else {
            window.open(window.location.pathname, "_self");
        }
    }

}
