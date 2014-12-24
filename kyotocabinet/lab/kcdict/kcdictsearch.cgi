#! /usr/bin/ruby
# -*- coding: utf-8 -*-

require 'cgi'

CMDNAME = 'kcdictmgr'
CMDOPTIONS = ""  # "-cz", "-co", or "-cx"
DICTS =
  [
   [ 'dict.kct', '' ],
#   [ 'eijiro-eiji.kct', 'Eijiro' ],
#   [ 'eijiro-waei.kct', 'Waeijiro' ],
#   [ 'eijiro-ryaku.kct', 'Ryakugoro' ],
#   [ 'eijiro-reiji.kct', 'Reijiro' ],
#   [ 'wordnet.kct', 'WordNet' ],
  ]
DICTGUESS = false
NOMATCHSUGGEST = true
TEXTSIMPLEMATCH = true
ENV['PATH'] = (ENV['PATH'] || '') + ':/usr/local/bin:.:..:../..'
ENV['LD_LIBRARY_PATH'] = (ENV['LD_LIBRARY_PATH'] || '') + ':/usr/local/lib:.:..:../..'

param = CGI.new
scriptname = ENV['SCRIPT_NAME']
scriptname = $0.sub(/.*\//, '') if !scriptname
p_query = (param['q'] || '').strip
p_dict = param['d'] || ''
p_dict = p_dict.empty? ? -1 : p_dict.to_i
p_mode = (param['m'] || '').strip
p_mode = 'p' if p_mode.empty?
p_num = (param['n'] || '').to_i
p_num = 30 if p_num < 1
p_num = 10000 if p_num > 10000
p_skip = (param['s'] || '').to_i
p_skip = 0 if p_skip < 0
p_skip = 65536 if p_skip > 65536
p_type =(param['t'] || '').strip

if DICTGUESS && p_dict < 0
  didx = p_query.match(/^[\x00-\x7f]+$/) ? 0 : 1
  dict = DICTS[didx]
else
  p_dict = 0 if p_dict < 0
  p_dict = 0 if p_dict >= DICTS.length
  dict = DICTS[p_dict]
end

ctype = p_type == 'x' ? 'application/xml' : 'text/html; charset=UTF-8'
printf("Content-Type: %s\r\n", ctype)
printf("\r\n")

print <<__EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<meta http-equiv="Content-Style-Type" content="text/css" />
<meta http-equiv="Content-Script-Type" content="text/javascript" />
<title>Dictionary Search</title>
<style tyle="text/css">html, body {
  margin: 0ex;
  background: #ffffff;
  text-align: center;
}
div#maindisplay {
  display: inline-block;
  width: 100ex;
  margin: 2.5ex 0ex;
  text-align: left;
  border: solid 1px #dddddd;
}
div#inputpanel {
  margin: 0ex;
  padding: 1ex 1.5ex;
  background: #eef8ff;
  border-bottom: solid 1px #ddeeff;
}
span#querypanel {
  margin-left: 1.5ex;
}
h1.logo {
  margin: 0.2ex 0ex;
  font-size: 110%;
}
h1.logo a {
  color: #aaddee;
  text-decoration: none;
}
input#inputquery {
  width: 35ex;
}
div#mainpanel {
  margin: 0ex;
  padding: 1ex 0.8ex;
}
a {
  color: #1144ee;
}
p.notice {
  margin: 1ex 1ex;
  color: #555555;
}
ul#suggestion {
  line-height: 120%;
}
div.credit {
  padding: 1ex 1ex;
  text-align: right;
  font-size: 80%;
  color: #889999;
}
div.credit a {
  color: #889999;
  text-decoration: none;
}
dl#result dt {
  margin: 1ex 0ex 0.2ex 0ex;
  line-height: 110%;
}
dl#result dt span.word {
  color: #001177;
}
dl#result dt span.seq {
  display: inline-block;
  width: 4ex;
  text-align: right;
  font-size: 70%;
  color: #777788;
}
dl#result dt span.part, dl#result dt span.dist {
  font-size: 80%;
  color: #666677;
}
dl#result dd {
  margin: 0.3ex 2ex 0.4ex 5.5ex;
}
dl#result dd.text {
  font-size: 95%;
  color: #223333;
}
div#navi {
  margin: 0.5ex 0.5ex;
  font-size: 95%;
}
div#navi a {
  margin: 0ex 1ex;
}
@media (max-device-width:480px) {
  div#maindisplay {
    margin: 0ex;
    width: 100%;
    font-size: 26pt;
  }
  span#querypanel, span#optionpanel {
    display: block;
    margin: 0ex;
    font-size: 120%;
  }
  input#inputquery {
    width: 30ex;
    font-size: 130%;
  }
  select {
    font-size: 105%;
    width: 9ex;
  }
  input#inputsubmit {
    font-size: 100%;
    width: 10ex;
  }
  dl#result dt {
    margin-left: 1ex;
    font-size: 110%;
  }
  dl#result dt span.seq {
    display: none;
  }
  dl#result dd {
    margin-left: 2.5ex;
  }
}
</style>
<script type="text/javascript">function init() {
  document.mainform.inputquery.focus();
}
</script>
</head>
<body onload="init();">
<div id="maindisplay">
__EOF

printf("<form action=\"%s\" method=\"GET\" name=\"mainform\" id=\"mainform\">\n",
       CGI.escapeHTML(scriptname))
printf("<div id=\"inputpanel\">\n")
printf("<h1 class=\"logo\"><a href=\"%s\">Dictionary Search</a></h1>\n",
       CGI.escapeHTML(scriptname))
printf("<span id=\"querypanel\">\n")
printf("<input type=\"text\" name=\"q\" value=\"%s\" size=\"32\" id=\"inputquery\" />\n",
       CGI.escapeHTML(p_query))
printf("</span>\n")
printf("<span id=\"optionpanel\">\n")
if DICTS.length > 1
  printf("<select name=\"d\" id=\"inputdict\" >\n")
  if DICTGUESS
    printf("<option value=\"-1\"%s>auto</option>\n",
           p_dict < 0 ? ' selected="selected"' : '')
  end
  dseq = 0
  DICTS.each do |pair|
    printf("<option value=\"%d\"%s>%s</option>\n",
           dseq, dseq == p_dict ? ' selected="selected"' : '', CGI.escapeHTML(pair[1]))
    dseq += 1
  end
  printf("</select>\n")
end
printf("<select name=\"m\" id=\"inputmode\" >\n")
[ [ 'p', 'prefix' ], [ 'f', 'full' ], [ 'a', 'ambiguous' ], [ 'm', 'middle' ],
  [ 'r', 'regex' ], [ 'tm', 'text middle' ], [ 'tr', 'text regex' ] ].each do |pair|
  mode = pair[0]
  printf("<option value=\"%s\"%s>%s</option>\n",
         mode, mode == p_mode ? ' selected="selected"' : '', pair[1])
end
printf("</select>\n")
printf("<select name=\"n\" id=\"inputmax\" >\n")
[ 1, 5, 10, 20, 30, 40, 50, 100, 200, 300, 400, 500, 1000 ].each do |num|
  printf("<option value=\"%d\"%s>%d</option>\n",
         num, num == p_num ? ' selected="selected"' : '', num)
end
printf("</select>\n")
printf("<input type=\"submit\" value=\"search\" id=\"inputsubmit\" />\n")
printf("</span>\n")
printf("</div>\n")
printf("</form>\n")

printf("<div id=\"mainpanel\">\n")
if !p_query.empty?
  case p_mode
  when 'f'
    mode = '-f'
  when 'a'
    mode = '-a'
  when 'm'
    mode = '-m'
  when 'r'
    mode = '-r'
  when 'tm'
    mode = '-tm'
  when 'tr'
    mode = '-tr'
  else
    mode = ''
  end
  tsmode = TEXTSIMPLEMATCH ? '-ts' : ''
  cmd = sprintf('%s search %s -max %d %s %s -iu -- %s "%s"', CMDNAME, CMDOPTIONS,
                p_num + p_skip + 1, mode, tsmode, dict[0], CGI.escape(p_query))
  records = []
  IO.popen(cmd) do |io|
    io.readlines.each do |line|
      line.force_encoding('utf-8')
      line = line.strip
      fields = line.split("\t")
      if fields.length >= 3
        dist = p_mode == 'a' && fields.length >= 4 ? fields[3] : nil
        record = [ fields[0], fields[1], fields[2], dist ]
        records.push(record)
      end
    end
  end
  if records.empty?
    printf("<p class=\"notice\">There's no matching entry.</p>\n")
    if NOMATCHSUGGEST && (p_mode == 'p' || p_mode == 'f')
      cmd = sprintf('%s search %s -max 20 -a -iu -pk -- %s "%s"', CMDNAME, CMDOPTIONS,
                    dict[0], CGI.escape(p_query))
      keys = []
      IO.popen(cmd) do |io|
        io.readlines.each do |line|
          line.force_encoding('utf-8')
          line = line.strip
          fields = line.split("\t")
          if fields.length >= 1
            keys.push(fields[0])
          end
        end
      end
      if keys.length > 0
        cnt = 0
        printf("<p class=\"notice\">Did you mean one of these words?</p>\n")
        printf("<ul id=\"suggestion\">\n")
        uniq = {}
        keys.each do |key|
          if cnt < 10 && !uniq[key]
            printf("<li><a href=\"%s?q=%s&amp;d=%d&amp;m=%s&amp;n=%d\">%s</a></li>\n",
                   scriptname, CGI.escape(key), p_dict, CGI.escape(p_mode), p_num,
                   CGI.escapeHTML(key))
            cnt += 1
            uniq[key] = true
          end
        end
        printf("</ul>\n")
      end
    end
  else
    isnext = records.length > p_num + p_skip
    records = records[p_skip, p_num] || []
    seq = p_skip
    printf("<dl id=\"result\">\n")
    records.each do |record|
      seq += 1
      printf("<dt id=\"r%d\">", seq)
      printf("<span class=\"seq\">%d:</span> ", seq)
      printf("<span class=\"word\">%s</span>", CGI.escapeHTML(record[0]))
      if !record[1].empty?
        printf(" <span class=\"part\">[%s]</span> ", CGI.escapeHTML(record[1]))
      end
      if record[3]
        printf(" <span class=\"dist\">(%d)</span> ", record[3])
      end
      printf("</dt>\n")
      printf("<dd class=\"text\">%s</dd>\n", CGI.escapeHTML(record[2]))
    end
    printf("</dl>\n")
    if p_skip > 0 || isnext
      printf("<div id=\"navi\">\n")
      if p_skip > 0
        printf("<a href=\"%s?q=%s&amp;d=%d&amp;m=%s&amp;n=%d&amp;s=%d\">PREV</a>\n",
               scriptname, CGI.escape(p_query), p_dict, CGI.escape(p_mode),
               p_num, p_skip - p_num)
      end
      if isnext
        printf("<a href=\"%s?q=%s&amp;d=%d&amp;m=%s&amp;n=%d&amp;s=%d\">NEXT</a>\n",
               scriptname, CGI.escape(p_query), p_dict, CGI.escape(p_mode),
               p_num, p_skip + p_num)
      end
      printf("</div>\n")
    end
  end
end
printf("</div>\n")

print <<__EOF
<div class="credit">powered by <a href="http://fallabs.com/kyotocabinet/">Kyoto Cabinet</a></div>
</div>
</body>
</html>
__EOF
