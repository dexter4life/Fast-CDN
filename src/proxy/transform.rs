//! Dynamic asset transformation (image resizing, format conversion).

use std::collections::HashMap;
use thiserror::Error;

/// Transform errors
#[derive(Debug, Error)]
pub enum TransformError {
    #[error("Invalid transform parameter: {0}")]
    InvalidParameter(String),

    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error("Image processing error: {0}")]
    ProcessingError(String),

    #[error("Output too large")]
    OutputTooLarge,
}

/// Supported output formats
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Jpeg,
    Png,
    WebP,
    Avif,
    Original,
}

impl OutputFormat {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "jpg" | "jpeg" => Some(Self::Jpeg),
            "png" => Some(Self::Png),
            "webp" => Some(Self::WebP),
            "avif" => Some(Self::Avif),
            "original" | "orig" => Some(Self::Original),
            _ => None,
        }
    }

    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Jpeg => "image/jpeg",
            Self::Png => "image/png",
            Self::WebP => "image/webp",
            Self::Avif => "image/avif",
            Self::Original => "application/octet-stream",
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            Self::Jpeg => "jpg",
            Self::Png => "png",
            Self::WebP => "webp",
            Self::Avif => "avif",
            Self::Original => "",
        }
    }
}

/// Resize fit mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FitMode {
    /// Preserve aspect ratio, fit within bounds
    #[default]
    Contain,
    /// Preserve aspect ratio, fill bounds and crop
    Cover,
    /// Exact dimensions (may distort)
    Fill,
    /// Resize only if larger than target
    ScaleDown,
}

impl FitMode {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "contain" | "inside" => Some(Self::Contain),
            "cover" | "outside" => Some(Self::Cover),
            "fill" | "stretch" => Some(Self::Fill),
            "scale-down" | "scaledown" => Some(Self::ScaleDown),
            _ => None,
        }
    }
}

/// Image transform parameters parsed from URL query
#[derive(Debug, Clone, Default)]
pub struct TransformParams {
    /// Target width
    pub width: Option<u32>,
    /// Target height
    pub height: Option<u32>,
    /// Size shorthand (e.g., sz=100 means 100x100)
    pub size: Option<u32>,
    /// Output format
    pub format: Option<OutputFormat>,
    /// JPEG/WebP quality (1-100)
    pub quality: Option<u8>,
    /// Fit mode
    pub fit: FitMode,
    /// Background color for padding (hex)
    pub background: Option<String>,
    /// Blur radius (0-250)
    pub blur: Option<f32>,
    /// Sharpen amount
    pub sharpen: Option<f32>,
    /// Rotation degrees (0, 90, 180, 270)
    pub rotate: Option<u16>,
    /// Flip horizontal
    pub flip_h: bool,
    /// Flip vertical
    pub flip_v: bool,
    /// Grayscale
    pub grayscale: bool,
    /// Auto-optimize based on Accept header
    pub auto_format: bool,
}

impl TransformParams {
    /// Parse transform parameters from query string
    pub fn from_query(query: &str) -> Self {
        let mut params = Self::default();
        
        if query.is_empty() {
            return params;
        }

        let map: HashMap<&str, &str> = query
            .split('&')
            .filter_map(|kv| {
                let mut parts = kv.splitn(2, '=');
                Some((parts.next()?, parts.next().unwrap_or("")))
            })
            .collect();

        // Parse width
        if let Some(w) = map.get("w").or(map.get("width")) {
            params.width = w.parse().ok();
        }

        // Parse height
        if let Some(h) = map.get("h").or(map.get("height")) {
            params.height = h.parse().ok();
        }

        // Parse size shorthand
        if let Some(sz) = map.get("sz").or(map.get("size")) {
            params.size = sz.parse().ok();
        }

        // Parse format
        if let Some(fmt) = map.get("fmt").or(map.get("format")).or(map.get("f")) {
            params.format = OutputFormat::from_str(fmt);
        }

        // Parse quality
        if let Some(q) = map.get("q").or(map.get("quality")) {
            if let Ok(quality) = q.parse::<u8>() {
                params.quality = Some(quality.min(100).max(1));
            }
        }

        // Parse fit mode
        if let Some(fit) = map.get("fit") {
            if let Some(mode) = FitMode::from_str(fit) {
                params.fit = mode;
            }
        }

        // Parse background
        if let Some(bg) = map.get("bg").or(map.get("background")) {
            params.background = Some(bg.to_string());
        }

        // Parse blur
        if let Some(blur) = map.get("blur") {
            if let Ok(b) = blur.parse::<f32>() {
                params.blur = Some(b.min(250.0).max(0.0));
            }
        }

        // Parse sharpen
        if let Some(sharpen) = map.get("sharpen") {
            params.sharpen = sharpen.parse().ok();
        }

        // Parse rotation
        if let Some(rot) = map.get("rotate").or(map.get("rot")) {
            if let Ok(r) = rot.parse::<u16>() {
                params.rotate = Some(r % 360);
            }
        }

        // Parse flips
        params.flip_h = map.contains_key("flip_h") || map.contains_key("fliph");
        params.flip_v = map.contains_key("flip_v") || map.contains_key("flipv");

        // Parse grayscale
        params.grayscale = map.contains_key("grayscale") || map.contains_key("gray");

        // Parse auto format
        params.auto_format = map.contains_key("auto") || map.contains_key("auto_format");

        params
    }

    /// Check if any transforms are requested
    pub fn is_empty(&self) -> bool {
        self.width.is_none()
            && self.height.is_none()
            && self.size.is_none()
            && self.format.is_none()
            && self.quality.is_none()
            && self.blur.is_none()
            && self.sharpen.is_none()
            && self.rotate.is_none()
            && !self.flip_h
            && !self.flip_v
            && !self.grayscale
            && !self.auto_format
    }

    /// Get effective width
    pub fn effective_width(&self) -> Option<u32> {
        self.width.or(self.size)
    }

    /// Get effective height
    pub fn effective_height(&self) -> Option<u32> {
        self.height.or(self.size)
    }

    /// Generate a cache key variant string for these params
    pub fn cache_variant(&self) -> String {
        let mut parts = Vec::new();

        if let Some(w) = self.effective_width() {
            parts.push(format!("w{}", w));
        }
        if let Some(h) = self.effective_height() {
            parts.push(format!("h{}", h));
        }
        if let Some(ref fmt) = self.format {
            parts.push(format!("f{}", fmt.extension()));
        }
        if let Some(q) = self.quality {
            parts.push(format!("q{}", q));
        }
        if self.fit != FitMode::default() {
            parts.push(format!("fit{:?}", self.fit));
        }
        if let Some(blur) = self.blur {
            parts.push(format!("blur{}", blur as u32));
        }
        if let Some(rot) = self.rotate {
            if rot != 0 {
                parts.push(format!("rot{}", rot));
            }
        }
        if self.flip_h {
            parts.push("fh".to_string());
        }
        if self.flip_v {
            parts.push("fv".to_string());
        }
        if self.grayscale {
            parts.push("gray".to_string());
        }

        parts.join("_")
    }

    /// Validate parameters
    pub fn validate(&self) -> Result<(), TransformError> {
        // Check dimensions
        if let Some(w) = self.effective_width() {
            if w == 0 || w > 10000 {
                return Err(TransformError::InvalidParameter(format!(
                    "Width must be between 1 and 10000, got {}",
                    w
                )));
            }
        }

        if let Some(h) = self.effective_height() {
            if h == 0 || h > 10000 {
                return Err(TransformError::InvalidParameter(format!(
                    "Height must be between 1 and 10000, got {}",
                    h
                )));
            }
        }

        // Check rotation
        if let Some(rot) = self.rotate {
            if rot % 90 != 0 {
                return Err(TransformError::InvalidParameter(format!(
                    "Rotation must be a multiple of 90, got {}",
                    rot
                )));
            }
        }

        Ok(())
    }
}

/// Image transformer (placeholder for actual implementation)
#[cfg(feature = "image-processing")]
pub struct ImageTransformer {
    max_input_size: usize,
    max_output_size: usize,
}

#[cfg(feature = "image-processing")]
impl ImageTransformer {
    pub fn new(max_input_size: usize, max_output_size: usize) -> Self {
        Self {
            max_input_size,
            max_output_size,
        }
    }

    /// Transform an image according to parameters
    pub async fn transform(
        &self,
        input: &[u8],
        params: &TransformParams,
        input_content_type: &str,
    ) -> Result<(Vec<u8>, String), TransformError> {
        // Validate input size
        if input.len() > self.max_input_size {
            return Err(TransformError::ProcessingError(format!(
                "Input too large: {} > {}",
                input.len(),
                self.max_input_size
            )));
        }

        params.validate()?;

        // Actual implementation would use the `image` crate here
        // For now, return the original
        Ok((input.to_vec(), input_content_type.to_string()))
    }
}

/// Select optimal format based on Accept header
pub fn select_format_from_accept(accept: &str, original_format: OutputFormat) -> OutputFormat {
    // Check for modern format support
    if accept.contains("image/avif") {
        return OutputFormat::Avif;
    }
    if accept.contains("image/webp") {
        return OutputFormat::WebP;
    }

    // Fall back to original or JPEG
    match original_format {
        OutputFormat::Png => OutputFormat::Png, // Keep PNG for transparency
        _ => OutputFormat::Jpeg,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_transform_params() {
        let params = TransformParams::from_query("w=100&h=200&fmt=webp&q=80");

        assert_eq!(params.width, Some(100));
        assert_eq!(params.height, Some(200));
        assert_eq!(params.format, Some(OutputFormat::WebP));
        assert_eq!(params.quality, Some(80));
    }

    #[test]
    fn test_size_shorthand() {
        let params = TransformParams::from_query("sz=150");

        assert_eq!(params.size, Some(150));
        assert_eq!(params.effective_width(), Some(150));
        assert_eq!(params.effective_height(), Some(150));
    }

    #[test]
    fn test_empty_params() {
        let params = TransformParams::from_query("");
        assert!(params.is_empty());

        let params = TransformParams::from_query("token=abc&expires=123");
        assert!(params.is_empty());
    }

    #[test]
    fn test_cache_variant() {
        let params = TransformParams::from_query("w=100&fmt=webp&q=75");
        let variant = params.cache_variant();

        assert!(variant.contains("w100"));
        assert!(variant.contains("fwebp"));
        assert!(variant.contains("q75"));
    }

    #[test]
    fn test_validate_params() {
        let params = TransformParams::from_query("w=100");
        assert!(params.validate().is_ok());

        let params = TransformParams::from_query("w=0");
        assert!(params.validate().is_err());

        let params = TransformParams::from_query("w=99999");
        assert!(params.validate().is_err());

        let params = TransformParams::from_query("rotate=45");
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_output_format() {
        assert_eq!(OutputFormat::from_str("jpg"), Some(OutputFormat::Jpeg));
        assert_eq!(OutputFormat::from_str("JPEG"), Some(OutputFormat::Jpeg));
        assert_eq!(OutputFormat::from_str("webp"), Some(OutputFormat::WebP));
        assert_eq!(OutputFormat::from_str("avif"), Some(OutputFormat::Avif));
        assert_eq!(OutputFormat::from_str("unknown"), None);
    }

    #[test]
    fn test_select_format() {
        let accept = "image/avif,image/webp,image/png,image/jpeg,*/*";
        assert_eq!(
            select_format_from_accept(accept, OutputFormat::Jpeg),
            OutputFormat::Avif
        );

        let accept = "image/webp,image/png,image/jpeg,*/*";
        assert_eq!(
            select_format_from_accept(accept, OutputFormat::Jpeg),
            OutputFormat::WebP
        );

        let accept = "image/jpeg,*/*";
        assert_eq!(
            select_format_from_accept(accept, OutputFormat::Jpeg),
            OutputFormat::Jpeg
        );
    }
}
