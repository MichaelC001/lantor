use axum::extract::Multipart;

use crate::{
    app::{to_string, CommandResult},
    application::messages::SendMessageRequest,
    attachments::{attachment_exceeds_size_limit, ATTACHMENT_SIZE_LIMIT_MIB},
    models::AttachmentUpload,
};

pub(crate) async fn parse_multipart_send_message(
    mut multipart: Multipart,
) -> CommandResult<SendMessageRequest> {
    let mut request = None;
    let mut attachments = Vec::new();
    while let Some(field) = multipart.next_field().await.map_err(to_string)? {
        let field_name = field.name().unwrap_or_default().to_owned();
        match field_name.as_str() {
            "request" => {
                if request.is_some() {
                    return Err(
                        "multipart send_message contains duplicate request fields".to_owned()
                    );
                }
                let metadata = field.text().await.map_err(to_string)?;
                request = Some(
                    serde_json::from_str::<SendMessageRequest>(&metadata)
                        .map_err(|err| format!("invalid send_message request metadata: {err}"))?,
                );
            }
            "attachments" => {
                let original_name = field
                    .file_name()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or("attachment")
                    .to_owned();
                let mime_type = field
                    .content_type()
                    .map(ToString::to_string)
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| "application/octet-stream".to_owned());
                let bytes = field.bytes().await.map_err(to_string)?;
                if attachment_exceeds_size_limit(bytes.len() as u64) {
                    return Err(format!(
                        "attachment {original_name} is larger than {ATTACHMENT_SIZE_LIMIT_MIB}MB"
                    ));
                }
                attachments.push(AttachmentUpload {
                    original_name,
                    mime_type,
                    bytes: bytes.to_vec(),
                });
            }
            _ => {
                return Err(format!(
                    "multipart send_message contains unknown field {field_name}"
                ));
            }
        }
    }

    let mut request = request
        .ok_or_else(|| "multipart send_message is missing the request metadata field".to_owned())?;
    if request
        .attachments
        .as_ref()
        .is_some_and(|attachments| !attachments.is_empty())
    {
        return Err(
            "multipart send_message request metadata must not contain attachments".to_owned(),
        );
    }
    request.attachments = Some(attachments);
    Ok(request)
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        extract::{FromRequest, Multipart},
        http::Request,
    };
    use uuid::Uuid;

    use super::parse_multipart_send_message;

    #[tokio::test]
    async fn multipart_send_message_preserves_binary_attachments() {
        let boundary = "lantor-attachment-boundary";
        let channel_id = Uuid::new_v4();
        let metadata = format!(
            r#"{{"channelId":"{channel_id}","threadRootId":null,"body":"hello","asTask":false}}"#
        );
        let mut body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"request\"\r\n\
             Content-Type: application/json\r\n\r\n\
             {metadata}\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"attachments\"; filename=\"probe.bin\"\r\n\
             Content-Type: application/octet-stream\r\n\r\n"
        )
        .into_bytes();
        body.extend_from_slice(&[0, 1, 254, 255]);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let request = Request::builder()
            .header(
                "content-type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body))
            .unwrap();

        let multipart = Multipart::from_request(request, &()).await.unwrap();
        let request = parse_multipart_send_message(multipart).await.unwrap();

        assert_eq!(request.channel_id, channel_id);
        assert_eq!(request.thread_root_id, None);
        assert_eq!(request.body, "hello");
        assert!(!request.as_task);
        let attachments = request.attachments.unwrap();
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].original_name, "probe.bin");
        assert_eq!(attachments[0].mime_type, "application/octet-stream");
        assert_eq!(attachments[0].bytes, [0, 1, 254, 255]);
    }
}
