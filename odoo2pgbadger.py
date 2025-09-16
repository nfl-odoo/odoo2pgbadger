
import regex
import argparse
from datetime import datetime

DT_FORMAT = '%Y-%m-%d %H:%M:%S'

def write(fout, out_buffer):
    if out_buffer and out_buffer[-1] != '\n':
        out_buffer += '\n'
    fout.write(out_buffer)
    return ''

def odoo2pgbadger_sql(line):
    """Parse Odoo SQL log lines into pgBadger format"""
    line_match = r'^(?P<dt>.{19}).*\[(?P<timer>.+) ms\] query:(?P<query>.+)$'
    match = regex.match(line_match, line)
    if not match:
        return ''
    return '%s [1]: LOG:  duration: %s ms  statement: %s' % (
        match['dt'],
        float(match['timer']),
        match['query'],
    )

def odoo2pgbadger_http(line):
    """Parse Odoo HTTP werkzeug log lines into pgBadger-like format"""
    line_match = (
        r'^(?P<dt>.{19}).*werkzeug: '
        r'(?P<ip>[0-9\.]+) .* "(?P<method>[A-Z]+) (?P<path>[^ ]+) HTTP/[0-9.]+" '
        r'(?P<status>[0-9]+) .* (?P<query_count>\d+) (?P<query_time>[0-9.]+) (?P<remaining_time>[0-9.]+)$'
    )
    match = regex.match(line_match, line)
    if not match:
        return ''
    # use "duration" field (second to last number)
    return '%s [1]: LOG:  duration: %s ms  statement: %s %s' % (
        match['dt'],
        float(match['remaining_time']) * 1000,  # convert sec to ms
        match['method'],
        match['path'],
    )

def startswithdate(line):
    try:
        datetime.strptime(line[:19], DT_FORMAT)
        return True
    except ValueError:
        return False
    except Exception as e:
        raise e

def main():
    parser = argparse.ArgumentParser(description="Parse Odoo logs into pgBadger-compatible logs.")
    parser.add_argument('-i', '--input', default='odoo.log', help='Input log file (default: odoo.log)')
    parser.add_argument('-o', '--output', default='odoo_parsed.log', help='Output log file (default: odoo_parsed.log)')
    parser.add_argument('-m', '--mode', choices=['sql', 'http'], default='sql',
                        help='Parsing mode: sql or http (default: sql)')
    args = parser.parse_args()

    if args.mode == 'sql':
        parser_fn = odoo2pgbadger_sql
    else:
        parser_fn = odoo2pgbadger_http

    c, pl = 0, 0
    out_buffer = ''

    with open(args.input, 'r') as fin, open(args.output, 'w+') as fout:
        for line in fin:
            c += 1
            if not c % 100000:
                print('Processed %s lines...' % c)

            if out_buffer:
                if startswithdate(line):
                    pl += 1
                    out_buffer = write(fout, out_buffer)
                else:
                    out_buffer += ' %s' % line.strip()

            if not out_buffer:
                out_buffer += parser_fn(line)

        if out_buffer:
            pl += 1
            fout.write(out_buffer)

    print('Total Lines: %s' % c)
    print('Parsed Lines: %s' % pl)
    print(f'pgbadger {args.output} -f stderr')

if __name__ == '__main__':
    main()
