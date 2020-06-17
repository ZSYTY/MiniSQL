/*
 * Use and distribution licensed under the Apache license version 2.
 *
 * See the COPYING file in the root project directory for full text.
 */

#include <sstream>

#include "sqltoast/sqltoast.h"

#include "parser/error.h"
#include "parser/parse.h"
#include "parser/sequence.h"

namespace sqltoast {

bool parse_drop_index(
        parse_context_t& ctx,
        token_t& cur_tok,
        std::unique_ptr<statement_t>& out) {
    lexer_t& lex = ctx.lexer;
    parse_position_t start = ctx.lexer.cursor;
    symbol_t cur_sym;
    lexeme_t index_name;

    cur_tok = lex.next();
    cur_sym = cur_tok.symbol;
    if (cur_sym != SYMBOL_INDEX) {
        // rewind
        ctx.lexer.cursor = start;
        return false;
    }

    cur_tok = lex.next();
    cur_sym = cur_tok.symbol;
    if (cur_sym == SYMBOL_IDENTIFIER) {
        index_name = cur_tok.lexeme;
        cur_tok = lex.next();
        goto statement_ending;
    }
    goto err_expect_identifier;
err_expect_identifier:
    expect_error(ctx, SYMBOL_IDENTIFIER);
    return false;
statement_ending:
    cur_sym = cur_tok.symbol;
    if (cur_sym == SYMBOL_SEMICOLON || cur_sym == SYMBOL_EOS)
        goto push_statement;
    expect_any_error(ctx, {SYMBOL_EOS, SYMBOL_SEMICOLON});
    return false;
push_statement:
    if (ctx.opts.disable_statement_construction)
        return true;
    out = std::make_unique<drop_index_statement_t>(index_name);
    return true;
}

} // namespace sqltoast
