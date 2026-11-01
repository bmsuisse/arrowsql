// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Parser for the COLINFO TDS token (0xA5).
//!
//! COLINFO carries per-column browse-mode information for cursor result sets.
//! The payload is length-prefixed (u16): per column `ColNum` (1-based),
//! `TableNum`, `Status` and, when status 0x20 is set, a B_VARCHAR `ColName`.
//! Status 0x10 marks a hidden column, which drivers must not expose.

use async_trait::async_trait;

use super::common::TokenParser;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{
    io::token_stream::ParserContext,
    token::tokens::{ColInfoToken, Tokens},
};

const STATUS_HIDDEN: u8 = 0x10;
const STATUS_DIFFERENT_NAME: u8 = 0x20;

/// Reads the COLINFO token's u16 length prefix and skips the body.
#[derive(Default)]
pub(crate) struct ColInfoTokenParser;

#[async_trait]
impl<T> TokenParser<T> for ColInfoTokenParser
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, _context: &ParserContext) -> TdsResult<Tokens> {
        let length = reader.read_uint16().await? as usize;
        let mut body = vec![0u8; length];
        let mut filled = 0;
        while filled < length {
            let read = reader.read_bytes(&mut body[filled..]).await?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        let mut hidden = Vec::new();
        let mut at = 0;
        while at + 3 <= filled {
            let (col, status) = (body[at], body[at + 2]);
            at += 3;
            if status & STATUS_DIFFERENT_NAME != 0 {
                // B_VARCHAR: one length byte counting UTF-16 code units.
                at += 1 + 2 * body.get(at).copied().unwrap_or(0) as usize;
            }
            if status & STATUS_HIDDEN != 0 {
                hidden.push(u16::from(col));
            }
        }
        Ok(Tokens::ColInfo(ColInfoToken { hidden }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::messages::PacketType;
    use crate::test_packet_support::{TestPacketBuilder, create_network_transport_with_data};

    #[tokio::test]
    async fn test_col_info_empty_body() {
        let mut builder = TestPacketBuilder::new(PacketType::PreLogin);
        builder.append_u16(0);
        let mut reader = create_network_transport_with_data(&builder.build());

        let parser = ColInfoTokenParser;
        let token = parser
            .parse(&mut reader, &ParserContext::default())
            .await
            .unwrap();
        assert!(matches!(token, Tokens::ColInfo(_)));
    }

    #[tokio::test]
    async fn test_col_info_with_body() {
        let mut builder = TestPacketBuilder::new(PacketType::PreLogin);
        builder.append_u16(4);
        builder.append_bytes(&[0xAA, 0xBB, 0xCC, 0xDD]);
        let mut reader = create_network_transport_with_data(&builder.build());

        let parser = ColInfoTokenParser;
        let token = parser
            .parse(&mut reader, &ParserContext::default())
            .await
            .unwrap();
        assert!(matches!(token, Tokens::ColInfo(_)));
    }
}
