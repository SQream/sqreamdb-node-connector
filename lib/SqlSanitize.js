// @ts-check
"use strict";

const keywords = Object.fromEntries([
  "add"
  ,"all"
  ,"alter"
  ,"and"
  ,"any"
  ,"as"
  ,"asc"
  ,"authorization"
  ,"backup"
  ,"begin"
  ,"between"
  ,"break"
  ,"browse"
  ,"bulk"
  ,"by"
  ,"cascade"
  ,"case"
  ,"check"
  ,"checkpoint"
  ,"close"
  ,"clustered"
  ,"collate"
  ,"column"
  ,"commit"
  ,"compute"
  ,"constraint"
  ,"containstable"
  ,"continue"
  ,"convert"
  ,"create"
  ,"cross"
  ,"current"
  ,"current_time"
  ,"current_user"
  ,"cursor"
  ,"database"
  ,"dbcc"
  ,"deallocate"
  ,"declare"
  ,"default"
  ,"delete"
  ,"deny"
  ,"desc"
  ,"disk"
  ,"distinct"
  ,"distributed"
  ,"double"
  ,"drop"
  ,"dump"
  ,"else"
  ,"end"
  ,"errlvl"
  ,"escape"
  ,"except"
  ,"exec"
  ,"execute"
  ,"exists"
  ,"exit"
  ,"external"
  ,"fetch"
  ,"file"
  ,"fillfactor"
  ,"for"
  ,"foreign"
  ,"freetext"
  ,"freetexttable"
  ,"from"
  ,"full"
  ,"function"
  ,"goto"
  ,"grant"
  ,"group"
  ,"hash"
  ,"having"
  ,"holdlock"
  ,"identity"
  ,"identity_insert"
  ,"identitycol"
  ,"if"
  ,"in"
  ,"index"
  ,"inner"
  ,"insert"
  ,"intersect"
  ,"into"
  ,"is"
  ,"join"
  ,"key"
  ,"kill"
  ,"left"
  ,"like"
  ,"limit"
  ,"lineno"
  ,"load"
  ,"loop"
  ,"merge"
  ,"national"
  ,"natural"
  ,"nocheck"
  ,"nonclustered"
  ,"not"
  ,"null"
  ,"nullif"
  ,"of"
  ,"off"
  ,"offsets"
  ,"on"
  ,"open"
  ,"opendatasource"
  ,"openquery"
  ,"openrowset"
  ,"openxml"
  ,"option"
  ,"or"
  ,"order"
  ,"outer"
  ,"over"
  ,"percent"
  ,"pivot"
  ,"precision"
  ,"primary"
  ,"print"
  ,"proc"
  ,"procedure"
  ,"raiserror"
  ,"read"
  ,"readtext"
  ,"reconfigure"
  ,"references"
  ,"replication"
  ,"restore"
  ,"restrict"
  ,"return"
  ,"revert"
  ,"revoke"
  ,"right"
  ,"rollback"
  ,"rowcount"
  ,"rowguidcol"
  ,"rule"
  ,"save"
  ,"schema"
  ,"securityaudit"
  ,"select"
  ,"session_user"
  ,"set"
  ,"setuser"
  ,"shutdown"
  ,"some"
  ,"statistics"
  ,"system_user"
  ,"table"
  ,"tablesample"
  ,"textsize"
  ,"then"
  ,"to"
  ,"top"
  ,"tran"
  ,"transaction"
  ,"trigger"
  ,"truncate"
  ,"tsequal"
  ,"union"
  ,"unique"
  ,"unpivot"
  ,"update"
  ,"updatetext"
  ,"use"
  ,"user"
  ,"values"
  ,"varying"
  ,"view"
  ,"waitfor"
  ,"when"
  ,"where"
  ,"while"
  ,"with"
  ,"writetext"
].map((k) => [k, true]));
keywords['current_date'] = true;
keywords['current_timestamp'] = true;
keywords['sysdate'] = true;

function extractStrings(original) {
  original = original || "";
  const res = {
    original: original,
    withPlaceholders: "",
    strings: [],
    semiColons: [],
    error: null,
  }

  let index = 0;
  let currentQuote = null;
  let dollarQuote = "";
  const chars = [];
  let s = "";
  let lineCount = 0;
  let charPos = -1;
  let quoteLine = -1;
  let quotePos = -1;
  while (index < original.length) {
    const char = original[index];
    if (char === "\n") {
      lineCount++;
      charPos = -1;
    }
    else {
      charPos++;
    }
    const next = original[index + 1];
    if (!currentQuote) {
      switch (char) {
        case '"':
        case "'":
          currentQuote = char;
          quoteLine = lineCount;
          quotePos = charPos;
          s = char;
          break;
        case "$":
          const found = original.indexOf("$", index + 1);
          if (found != -1 && /^[a-zA-Z0-9_]*$/.test(original.substr(index + 1, found - index - 1))) {
            s = original.substr(index, found - index + 1);
            dollarQuote = s;
            currentQuote = "$";
            quoteLine = lineCount;
            quotePos = charPos;
            index = found;
          } else {
            chars.push(char);
          }
          break;
        case "/":
          if (next === '*') {
            currentQuote = '*';
            quoteLine = lineCount;
            quotePos = charPos;
            s = char + '*';
            index++;
          } else {
            chars.push(char);
          }
          break;
        case "-":
          if (next === '-') {
            currentQuote = "\n";
          } else {
            chars.push(char);
          }
          break;
        case ";":
          res.semiColons.push(index);
          chars.push(char);
          break;
        default: 
          chars.push(char);
      }
    } else {
      s += char;
      if (char === currentQuote) {
        if (currentQuote == "\n") {
          currentQuote = null;
          chars.push("\n");
        } else if (currentQuote == "*") {
          if (next == "/") {
            index++;
            currentQuote = null;
          }
        } else if (next == currentQuote && ["'", '"'].includes(currentQuote)) {
          s += next;
          index++;
        } else if (currentQuote !== "$") {
          chars.push(`"{${res.strings.length}}"`);
          res.strings.push(s);
          currentQuote = null;
        } else if (currentQuote === "$") {
          const found = original.indexOf("$", index + 1);
          if (found != -1 && original.substr(index, found - index + 1) === dollarQuote) {
            s = `'${s.substr(dollarQuote.length, s.length - dollarQuote.length - 1).replace(/'/g, "''")}'`;
            index += dollarQuote.length - 1;
            chars.push(`"{${res.strings.length}}"`);
            res.strings.push(s);
            currentQuote = null;
          }
        }
      }
    }
    index++;
  }
  if (currentQuote === "\n") {
  } else if (currentQuote) {
    const name = {
      "'": "single quote",
      "\"": "double quote",
      "$": "dollar quote",
      "*": "comment block"
    }[currentQuote];
    res.error = new Error(`Unclosed ${name} at ${quoteLine + 1}:${quotePos + 1}. \n${s.substr(0, 20)}...`);
  }
  res.withPlaceholders = chars.join("");
  return res;
}

function sqlSanitize(sql, replacements = []) {
  const reps = [...replacements].map((s) => (typeof s === "undefined") ? "" : s);
  const es = extractStrings(sql);
  if (es.error) throw es.error;
  let words = [];
  let statements = es.withPlaceholders
    .split(";")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => {
      s = s.replace(/(%(s|d|i|b)|("{[0-9]+}"))/g, (match, found, type, ph) => {
        if (type) {
          if(reps.length === 0) return match;
          const rep = reps.shift();
          if (rep === null) return "NULL";
          switch (type) {
            case "d":
              return rep.toString().replace(/[^\de+\-.]/g, "");
            case "s":
              return `'${rep.toString().replace(/'/g, "''")}'`;
            case "i":
              return rep.toString().match(/[^a-z0-9_]/) || keywords[rep.toString()] ? `"${rep.toString().replace(/"/g, '""')}"` : rep.toString();
            case "b":
              return rep ? 'TRUE' : 'FALSE';
            default:
              return match;
          }
        } else {
          return es.strings.shift();
        }
      });
      return s;
    });
  return {statements, words};
}

module.exports = {
  extractStrings,
  sqlSanitize
}