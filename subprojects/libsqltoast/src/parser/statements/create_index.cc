/*
 * Use and distribution licensed under the Apache license version 2.
 *
 * See the COPYING file in the root project directory for full text.
 */

#include <sstream>

#include "sqltoast/sqltoast.h"

#include "parser/parse.h"
#include "parser/error.h"

namespace sqltoast {

bool parse_create_index(
        parse_context_t& ctx,
        token_t& cur_tok,
        std::unique_ptr<statement_t>& out) {
    lexer_t& lex = ctx.lexer;
    parse_position_t start = ctx.lexer.cursor;
    symbol_t cur_sym;
    lexeme_t index_name, table_name;
    std::vector<lexeme_t> col_list;

    cur_tok = lex.next();
    cur_sym = cur_tok.symbol;
    switch (cur_sym) {
        case SYMBOL_ERROR:
            return false;
        case SYMBOL_INDEX:
            cur_tok = lex.next();
            goto expect_index_name;
        default:
            // rewind
            ctx.lexer.cursor = start;
            return false;
    }
expect_index_name:
    cur_sym = cur_tok.symbol;
    if (cur_sym == SYMBOL_IDENTIFIER) {
        index_name = cur_tok.lexeme;
        cur_tok = lex.next();
        goto expect_on;
    }
    goto err_expect_identifier;
expect_on:
    cur_sym = cur_tok.symbol;
    if (cur_sym == SYMBOL_ON) {
        cur_tok = lex.next();
        goto expect_table_name;
    }
    goto err_expect_on;
expect_table_name:
    cur_sym = cur_tok.symbol;
    if (cur_sym == SYMBOL_IDENTIFIER) {
        table_name = cur_tok.lexeme;
        cur_tok = lex.next();
        goto expect_column_list_open;
    }
    goto err_expect_identifier;
err_expect_on:
    expect_error(ctx, SYMBOL_ON);
    return false;
err_expect_identifier:
    expect_error(ctx, SYMBOL_IDENTIFIER);
    return false;
expect_column_list_open:
    cur_sym = cur_tok.symbol;
    if (cur_sym == SYMBOL_LPAREN) {
        cur_tok = lex.next();
        goto process_column_list_item;
    }
    goto err_expect_lparen;
err_expect_lparen:
    expect_error(ctx, SYMBOL_LPAREN);
    return false;
process_column_list_item:
    cur_sym = cur_tok.symbol;
    if (cur_sym != SYMBOL_IDENTIFIER)
        goto err_expect_identifier;
    col_list.emplace_back(cur_tok.lexeme);
    cur_tok = lex.next();
    cur_sym = cur_tok.symbol;
    if (cur_sym == SYMBOL_COMMA) {
        cur_tok = lex.next();
        goto process_column_list_item;
    } else if (cur_sym == SYMBOL_RPAREN) {
        cur_tok = lex.next();
        goto statement_ending;
    }
    goto err_expect_comma_or_rparen;
err_expect_comma_or_rparen:
    expect_any_error(ctx, {SYMBOL_COMMA, SYMBOL_RPAREN});
    return false;
statement_ending:
    // We get here after successfully parsing the statement and now expect
    // either the end of parse content or a semicolon to indicate end of
    // statement.
    cur_sym = cur_tok.symbol;
    if (cur_sym == SYMBOL_SEMICOLON || cur_sym == SYMBOL_EOS)
        goto push_statement;
    expect_any_error(ctx, {SYMBOL_EOS, SYMBOL_SEMICOLON});
    return false;
push_statement:
    if (ctx.opts.disable_statement_construction)
        return true;
    out = std::make_unique<create_index_statement_t>(
            index_name, table_name, std::move(col_list));
    return true;
}

} // namespace sqltoast
