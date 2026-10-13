CREATE TABLE IF NOT EXISTS reviews (
  review_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  rating INTEGER,
  comment TEXT,
  review_time TIMESTAMP,
  sentiment TEXT,
  processed_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_reviews_product ON reviews(product_id);
CREATE INDEX IF NOT EXISTS idx_reviews_sentiment ON reviews(sentiment);
