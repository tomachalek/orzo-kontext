/*
 * Filter out bot visits, remove duplicities in KonText app log
 */

/// <ref path="./orzojs.d.ts" />

var outputDir = 'd:/work/data/kontext-logs/cleaned.tmp';


function containsAll(str) {
    var srch = Array.prototype.slice.call(arguments, 1);
    return srch.reduce(function (prev, curr) {
        return prev && str.indexOf(curr) > -1;
    }, true);
}


function agentIsMonitor(agentStr) {
    /*
    Python-urllib/2.7
    Zabbix-test
    */
    agentStr = agentStr ? agentStr.toLowerCase() : '';
    return containsAll(agentStr, 'python-urllib/2.7')
        || containsAll(agentStr, 'zabbix-test');
}

/**
 *
 */
function agentIsBot(agentStr) {
    /*
    Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
    Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)
    Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)
    Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)
    Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)
    Mozilla/5.0 (compatible; SeznamBot/3.2; +http://fulltext.sblog.cz/)
    Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)
    */
    agentStr = agentStr ? agentStr.toLowerCase() : '';
    return containsAll(agentStr, 'googlebot')
        || containsAll(agentStr, 'ahrefsbot')
        || containsAll(agentStr, 'yandexbot')
        || containsAll(agentStr, 'yahoo', 'slurp')
        || containsAll(agentStr, 'baiduspider')
        || containsAll(agentStr, 'seznambot')
        || containsAll(agentStr, 'bingbot');
}

dataChunks(2, function (idx) {
    //var d1 = 'd:/work/data/kontext-logs/test-merge/apache';
    var d1 = 'd:/work/data/kontext-logs/apache2015/0';
    return orzo.directoryReader(d1, idx);
});


applyItems(function (chunk, map) {
    while (chunk.hasNext()) {
        map(chunk.next());
    }
});


map(function (item) {
    var fr1 = orzo.fileReader(item),
        line;
    while (fr1.hasNext()) {
        line = fr1.next();
        if (line && (line.indexOf('"GET /files') > 30 || agentIsBot(line) || agentIsMonitor(line))) {
            continue;
        }
        emit(orzo.hash.md5(line).substr(0, 15), line);
    }
    orzo.print('done in ' + env.workerId);
});


reduce(2, function (key, values) {
    emit('output', values[0]);
});


finish(function (results) {
    doWith(orzo.fileWriter(outputDir + '/apache-clean.log.2'), function (fw) {
        results.get('output').forEach(function (item) {
            fw.writeln(item);
        });
    });
});