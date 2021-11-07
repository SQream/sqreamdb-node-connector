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
    comments: [],
  }

  let index = 0;
  let currentQuote = null;
  let dollarQuote = "";
  const chars = [];
  let currentString = [];
  let lineStart = 0;
  let lineCount = 0;
  let charPos = -1;
  let quoteLine = -1;
  let quotePos = -1;
  const split = original.split(/(\w+|\n|[ \t]+|.)/g).filter(Boolean);
  let len = 0;
  const indexes = split.map((str) => {
    return len += str.length;
  });
  indexes.unshift(0);
  const indexOf = (search, from = 0) => {
    for (let i = from; i < split.length; i++) {
      if (search === split[i]) {
        return i;
      }
    }
    return -1;
  };
  const substring = (from, to) => {
    return original.substring(indexes[from], indexes[to]);
  };
  while (index < split.length) {
    const char = split[index];
    const next = split[index + 1];
    if (char === "\n") {
      lineStart = index + 1;
      lineCount++;
      charPos = -1;
    } else {
      charPos = indexes[index] - indexes[lineStart];
    }
    if (!currentQuote) {
      switch (char) {
        case '"':
        case "'":
          currentQuote = char;
          quoteLine = lineCount;
          quotePos = charPos;
          currentString =[char];
          break;
        case "$":
          const found = indexOf("$", index + 1);
          if (found != -1 && /^[a-zA-Z0-9_]*$/.test(substring(index + 1, found))) {
            dollarQuote = substring(index, found + 1);
            currentString = [dollarQuote];
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
            currentString = [char, '*'];
            index++;
          } else {
            chars.push(char);
          }
          break;
        case "-":
          if (next === '-') {
            currentQuote = "\n";
            currentString = ['-'];
          } else {
            chars.push(char);
          }
          break;
        case ";":
          res.semiColons.push(indexes[index]);
          chars.push(char);
          break;
        default: 
          chars.push(char);
      }
    } else {
      currentString.push(char);
      if (char === currentQuote) {
        if (currentQuote === "\n") {
          currentQuote = null;
          chars.push(`/*{${res.comments.length}}*/`);
          res.comments.push(currentString.join(""));
        } else if (currentQuote === "*") {
          if (next === "/") {
            index++;
            currentQuote = null;
            chars.push(`/*{${res.comments.length}}*/`);
            res.comments.push(currentString.join("") + "/");
          }
        } else if (next === currentQuote && ["'", '"'].includes(currentQuote)) {
          currentString.push(next);
          index++;
        } else if (currentQuote !== "$") {
          chars.push(`"{${res.strings.length}}"`);
          res.strings.push(currentString.join(""));
          currentQuote = null;
        } else if (currentQuote === "$") {
          const found = indexOf("$", index + 1);
          if (found !== -1 && substring(index, found + 1) === dollarQuote) {
            index = found;
            chars.push(`"{${res.strings.length}}"`);
            res.strings.push(currentString.slice(0, -1).join("") + dollarQuote);
            currentQuote = null;
          } else if (found !== -1) {
            currentString.push(substring(index + 1, found));
            index = found - 1;
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
    res.error = new Error(`Unclosed ${name} at row ${quoteLine + 1}, col ${quotePos + 1}.\n\n${currentString.join("").substr(0, 20)}...`);
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
      s = s.replace(/(%(s|d|i|b)|("{[0-9]+}"|\/\*{[0-9]+}\*\/))/g, (match, found, type, ph) => {
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
          if (ph[0] === '"') {
            return es.strings.shift();
          } else {
            return es.comments.shift();
          }
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