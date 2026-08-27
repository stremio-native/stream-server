use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ImdbResult {
    d: Option<Vec<ImdbNode>>,
}

#[derive(Debug, Deserialize)]
struct ImdbNode {
    id: String,
    _l: String,         // Label (Title)
    y: Option<i32>,     // Year
    _q: Option<String>, // Type (feature, TV series, etc)
}

pub async fn resolve_imdb(
    client: &Client, title: &str, year: Option<i32>, type_: &str,
) -> Option<String> {
    let _ = type_; // Might use later for filtering
    let safe_title = urlencoding::encode(title);
    let first_char = title.chars().next().unwrap_or('a').to_ascii_lowercase();
    let url = format!(
        "https://sg.media-imdb.com/suggests/{}/{}.json",
        first_char, safe_title
    );

    // IMDB returns JSONP, e.g., imdb$frozen({"d": ...})
    // We need to strip the callback wrapper
    if let Ok(resp) = client.get(&url).send().await
        && let Ok(text) = resp.text().await
        && let Some(start) = text.find("({")
    {
        let end = text.rfind("})").unwrap_or(text.len());
        if start + 1 < end + 1 {
            let json_part = &text[start + 1..end + 1];
            if let Ok(result) = serde_json::from_str::<ImdbResult>(json_part)
                && let Some(nodes) = result.d
            {
                // Simple matching logic
                for node in nodes {
                    if let Some(node_year) = node.y {
                        if let Some(target_year) = year {
                            if (node_year - target_year).abs() <= 1 {
                                return Some(node.id);
                            }
                        } else {
                            // If no year specified, return first match?
                            // Ideally we want to be stricter
                            return Some(node.id);
                        }
                    }
                }
            }
        }
    }
    None
}
