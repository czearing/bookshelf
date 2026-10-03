use anyhow::{Context, Result, ensure};
use image::{ImageReader, imageops::FilterType};
use serde::Serialize;
use std::io::Cursor;

pub const MAX_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_DISTANCE: u32 = 8;
pub const MIN_MARGIN: u32 = 4;
const MAX_COLOR_ERROR: f64 = 0.12;

#[derive(Clone, Debug)]
pub struct Signature {
    /// Pixel size of the original image.
    pub width: u32,
    pub height: u32,
    /// Share of the (downscaled) image that is near white, and share that is grey
    /// (no colour). Together they identify a text page scanned in place of a cover.
    white_share: f64,
    grey_share: f64,
    average: u64,
    difference: u64,
    pixels: Vec<[u8; 3]>,
    aspect: f64,
    informative: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct Comparison {
    pub average_distance: u32,
    pub difference_distance: u32,
    pub color_error: f64,
    pub aspect_error: f64,
    pub informative: bool,
    pub strong: bool,
}

impl Comparison {
    pub fn distance(&self) -> u32 {
        self.average_distance.max(self.difference_distance)
    }
}

pub fn signature(data: &[u8]) -> Result<Signature> {
    ensure!(data.len() <= MAX_BYTES, "Cover image exceeds byte limit");
    let reader = || ImageReader::new(Cursor::new(data)).with_guessed_format();
    let (width, height) = reader()?
        .into_dimensions()
        .context("Invalid cover image header")?;
    ensure!(
        width >= 32 && height >= 32 && u64::from(width) * u64::from(height) <= 16_000_000,
        "Cover dimensions are too small or exceed the 16 megapixel limit"
    );
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(96 * 1024 * 1024);
    limits.max_image_width = Some(8192);
    limits.max_image_height = Some(8192);
    let mut decoder = reader()?;
    decoder.limits(limits);
    let decoded = decoder
        .decode()
        .context("Cannot decode cover image (JPEG, PNG, WebP supported)")?;
    let small = decoded
        .resize_exact(32, 32, FilterType::Triangle)
        .to_rgba8();
    let rgb = image::RgbImage::from_fn(32, 32, |x, y| {
        let pixel = small.get_pixel(x, y).0;
        let alpha = u32::from(pixel[3]);
        image::Rgb(std::array::from_fn(|i| {
            ((u32::from(pixel[i]) * alpha + 255 * (255 - alpha)) / 255) as u8
        }))
    });
    let gray = image::DynamicImage::ImageRgb8(rgb.clone()).into_luma8();
    let average_pixels = image::imageops::resize(&gray, 8, 8, FilterType::Lanczos3);
    let sum: u32 = average_pixels.pixels().map(|p| u32::from(p[0])).sum();
    let average = average_pixels.pixels().fold(0u64, |hash, pixel| {
        (hash << 1) | u64::from(u32::from(pixel[0]) * 64 > sum)
    });
    let difference_pixels = image::imageops::resize(&gray, 9, 8, FilterType::Lanczos3);
    let mut difference = 0u64;
    for y in 0..8 {
        for x in 0..8 {
            difference = (difference << 1)
                | u64::from(difference_pixels[(x, y)][0] < difference_pixels[(x + 1, y)][0]);
        }
    }
    let mean = gray.pixels().map(|p| f64::from(p[0])).sum::<f64>() / 1024.0;
    let variance = gray
        .pixels()
        .map(|p| (f64::from(p[0]) - mean).powi(2))
        .sum::<f64>()
        / 1024.0;
    let mut edges = 0;
    for y in 0..32 {
        for x in 0..32 {
            let value = gray[(x, y)][0];
            if x > 0 && value.abs_diff(gray[(x - 1, y)][0]) > 15 {
                edges += 1;
            }
            if y > 0 && value.abs_diff(gray[(x, y - 1)][0]) > 15 {
                edges += 1;
            }
        }
    }
    let samples = (rgb.width() * rgb.height()) as f64;
    let white_share = rgb
        .pixels()
        .filter(|p| p.0.iter().all(|c| *c >= 215))
        .count() as f64
        / samples;
    let grey_share = rgb
        .pixels()
        .filter(|p| p.0.iter().max().unwrap_or(&0) - p.0.iter().min().unwrap_or(&0) <= 24)
        .count() as f64
        / samples;
    Ok(Signature {
        width,
        height,
        white_share,
        grey_share,
        average,
        difference,
        pixels: rgb.pixels().map(|pixel| pixel.0).collect(),
        aspect: f64::from(width) / f64::from(height),
        informative: variance >= 100.0
            && edges >= 80
            && (8..=56).contains(&average.count_ones())
            && (8..=56).contains(&difference.count_ones()),
    })
}

impl Signature {
    /// Height divided by width: about 1.5 for a book cover.
    pub fn height_to_width(&self) -> f64 {
        f64::from(self.height) / f64::from(self.width)
    }

    pub fn informative(&self) -> bool {
        self.informative
    }
}

pub fn compare(local: &Signature, remote: &Signature) -> Comparison {
    let average_distance = (local.average ^ remote.average).count_ones();
    let difference_distance = (local.difference ^ remote.difference).count_ones();
    let color_error = local
        .pixels
        .iter()
        .zip(&remote.pixels)
        .flat_map(|(a, b)| a.iter().zip(b))
        .map(|(a, b)| f64::from(a.abs_diff(*b)))
        .sum::<f64>()
        / (32.0 * 32.0 * 3.0 * 255.0);
    let aspect_error = (local.aspect / remote.aspect - 1.0).abs();
    let informative = local.informative && remote.informative;
    Comparison {
        average_distance,
        difference_distance,
        color_error,
        aspect_error,
        informative,
        strong: informative
            && average_distance <= MAX_DISTANCE
            && difference_distance <= MAX_DISTANCE
            && color_error <= MAX_COLOR_ERROR
            && aspect_error <= 0.15,
    }
}

/// Why a candidate image is not good enough to be a book's cover.
pub struct CoverRequirements {
    /// Narrowest acceptable width in pixels. Providers never scale an image up
    /// beyond its original, so a narrow answer means the original is small.
    pub min_width: u32,
}

/// The picture Google serves when a book has no cover, at one reference size. It is
/// the same drawing at every size and in both PNG and JPEG, so it is recognised by
/// how it looks (a scale-independent perceptual comparison), not by its bytes.
const NOT_AVAILABLE: &[u8] = include_bytes!("../assets/google-image-not-available.png");

fn looks_like_not_available(candidate: &Signature) -> bool {
    use std::sync::OnceLock;
    static REFERENCE: OnceLock<Option<Signature>> = OnceLock::new();
    let Some(reference) = REFERENCE.get_or_init(|| signature(NOT_AVAILABLE).ok()) else {
        return false;
    };
    let c = compare(reference, candidate);
    c.average_distance <= 6 && c.difference_distance <= 6 && c.color_error <= 0.06
}

/// Accepts only an image that is plausibly the front of a book: decodable, portrait
/// (not a strip, banner or landscape crop), wide enough, with real content, and
/// not a provider's "image not available" picture.
pub fn judge_cover(data: &[u8], needs: &CoverRequirements) -> Result<Signature> {
    let signature = signature(data)?;
    let shape = signature.height_to_width();
    ensure!(
        (1.2..=1.9).contains(&shape),
        "Not a portrait book cover ({}x{}): likely a crop or banner",
        signature.width,
        signature.height
    );
    ensure!(
        signature.width >= needs.min_width,
        "Cover is only {}x{} pixels (needs {} wide): the original is small",
        signature.width,
        signature.height,
        needs.min_width
    );
    ensure!(
        !looks_like_not_available(&signature),
        "Provider's image-not-available picture"
    );
    ensure!(signature.informative(), "Cover image has no real content");
    ensure!(
        !(signature.white_share >= 0.80 && signature.grey_share >= 0.95),
        "Looks like a page of text, not a cover"
    );
    Ok(signature)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(image: &image::DynamicImage, format: image::ImageFormat) -> Vec<u8> {
        let mut bytes = Cursor::new(Vec::new());
        image.write_to(&mut bytes, format).unwrap();
        bytes.into_inner()
    }

    fn patterned(seed: u32) -> image::DynamicImage {
        image::DynamicImage::ImageRgb8(image::RgbImage::from_fn(160, 240, |x, y| {
            let cell = (x / 16) + 10 * (y / 16);
            let value =
                cell.wrapping_mul(0x9e3779b9).rotate_left(seed) ^ seed.wrapping_mul(0x85ebca6b);
            image::Rgb([(value >> 16) as u8, (value >> 8) as u8, value as u8])
        }))
    }

    #[test]
    fn resized_reencoded_cover_matches_but_different_art_does_not() {
        let original = patterned(3);
        let local = signature(&encode(&original, image::ImageFormat::Png)).unwrap();
        let thumbnail = original.resize_exact(80, 120, FilterType::Triangle);
        let remote = signature(&encode(&thumbnail, image::ImageFormat::Jpeg)).unwrap();
        let positive = compare(&local, &remote);
        assert!(positive.strong, "{positive:?}");
        let other = signature(&encode(&patterned(11), image::ImageFormat::Png)).unwrap();
        let negative = compare(&local, &other);
        assert!(!negative.strong, "{negative:?}");
    }

    #[test]
    fn blank_and_transparent_images_cannot_corroborate_identity() {
        for alpha in [0, 255] {
            let image = image::DynamicImage::ImageRgba8(image::RgbaImage::from_pixel(
                160,
                240,
                image::Rgba([120, 120, 120, alpha]),
            ));
            let hash = signature(&encode(&image, image::ImageFormat::Png)).unwrap();
            assert!(!compare(&hash, &hash).strong);
        }
        assert!(signature(b"not an image").is_err());
    }

    #[test]
    fn cover_gate_rejects_placeholders_at_any_scale_strips_and_small_originals() {
        let needs = CoverRequirements { min_width: 500 };
        let placeholder = image::load_from_memory(NOT_AVAILABLE).unwrap();
        for (width, format) in [
            (575, image::ImageFormat::Png),
            (600, image::ImageFormat::Jpeg),
            (1200, image::ImageFormat::Jpeg),
            (1917, image::ImageFormat::Jpeg),
        ] {
            let height = width * 783 / 600;
            let resized = placeholder.resize_exact(width, height, FilterType::Triangle);
            let error = judge_cover(&encode(&resized, format), &needs).unwrap_err();
            assert!(
                format!("{error:#}").contains("not-available"),
                "{width} {format:?}: {error:#}"
            );
        }
        // A 2000x320 strip is a crop, not a cover.
        let strip = image::DynamicImage::ImageRgb8(image::RgbImage::from_fn(2000, 320, |x, y| {
            image::Rgb([(x % 251) as u8, (y % 241) as u8, ((x + y) % 239) as u8])
        }));
        let error = judge_cover(&encode(&strip, image::ImageFormat::Jpeg), &needs).unwrap_err();
        assert!(format!("{error:#}").contains("portrait"));
        // A scanned page of thin black text on white is not a cover.
        let page = image::DynamicImage::ImageRgb8(image::RgbImage::from_fn(1200, 1800, |x, y| {
            let ink = (y % 120) < 9 && (x / 7) % 4 != 0 && x > 200 && x < 1000;
            if ink {
                image::Rgb([20, 20, 20])
            } else {
                image::Rgb([252, 252, 252])
            }
        }));
        let verdict = judge_cover(&encode(&page, image::ImageFormat::Jpeg), &needs);
        let message = verdict.err().map(|e| format!("{e:#}")).unwrap_or_default();
        assert!(
            message.contains("page of text") || message.contains("real content"),
            "a text page must be rejected, got: {message:?}"
        );
        // A real portrait cover that is too small to be good.
        let small = image::DynamicImage::ImageRgb8(image::RgbImage::from_fn(300, 450, |x, y| {
            image::Rgb([(x % 251) as u8, (y % 241) as u8, ((x * y) % 239) as u8])
        }));
        let error = judge_cover(&encode(&small, image::ImageFormat::Png), &needs).unwrap_err();
        assert!(format!("{error:#}").contains("small"));
        // A genuine, large, busy portrait image passes.
        let good = image::DynamicImage::ImageRgb8(image::RgbImage::from_fn(1200, 1800, |x, y| {
            let cell = (x / 120) + 10 * (y / 120);
            let value = cell.wrapping_mul(0x9e37_79b9).rotate_left(3) ^ 0x85eb_ca6b;
            image::Rgb([(value >> 16) as u8, (value >> 8) as u8, value as u8])
        }));
        let kept = judge_cover(&encode(&good, image::ImageFormat::Jpeg), &needs).unwrap();
        assert_eq!((kept.width, kept.height), (1200, 1800));
    }
}
