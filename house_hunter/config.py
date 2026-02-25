import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SearchConfig:
    """Configuration for a property search."""

    location: str = "San Francisco, CA"
    listing_type: str = "for_sale"
    property_type: Optional[list[str]] = None

    # Price
    price_min: Optional[int] = None
    price_max: Optional[int] = None

    # Beds / Baths
    beds_min: Optional[int] = None
    beds_max: Optional[int] = None
    baths_min: Optional[float] = None
    baths_max: Optional[float] = None

    # Square footage
    sqft_min: Optional[int] = None
    sqft_max: Optional[int] = None

    # Lot size
    lot_sqft_min: Optional[int] = None
    lot_sqft_max: Optional[int] = None

    # Year built
    year_built_min: Optional[int] = None
    year_built_max: Optional[int] = None

    # Time filters
    past_days: Optional[int] = None

    # Other
    foreclosure: Optional[bool] = None
    exclude_pending: bool = False
    mls_only: bool = False
    extra_property_data: bool = True
    limit: int = 500

    # Output
    output_csv: str = "results.csv"


@dataclass
class LLMConfig:
    """Configuration for the LLM provider.

    For llama-server (llama.cpp), just set api_base:
        HOUSE_HUNTER_API_BASE=http://localhost:8081/v1

    The model field is sent in the API request but llama-server ignores it
    (it serves whatever model is loaded). We still track it for display.

    For cloud APIs (OpenAI, Anthropic), set the model and the provider's
    own API key env var (OPENAI_API_KEY, ANTHROPIC_API_KEY, etc.).
    """

    model: str = "local"
    api_base: Optional[str] = None
    api_key: Optional[str] = None
    temperature: float = 0.3
    max_tokens: int = 4096
    batch_size: int = 20

    @classmethod
    def from_env(cls) -> "LLMConfig":
        api_base = os.environ.get("HOUSE_HUNTER_API_BASE")
        model = os.environ.get("HOUSE_HUNTER_MODEL")

        # Default model depends on whether we're hitting a local server
        if model is None:
            model = "local" if api_base else "gpt-4o-mini"

        return cls(
            model=model,
            api_base=api_base,
            api_key=os.environ.get("HOUSE_HUNTER_API_KEY"),
            temperature=float(os.environ.get("HOUSE_HUNTER_TEMPERATURE", "0.3")),
            max_tokens=int(os.environ.get("HOUSE_HUNTER_MAX_TOKENS", "4096")),
            batch_size=int(os.environ.get("HOUSE_HUNTER_BATCH_SIZE", "20")),
        )

    @property
    def is_local(self) -> bool:
        return self.api_base is not None

    @property
    def litellm_model(self) -> str:
        """Model string formatted for litellm."""
        # If pointing at a local server, use openai/ prefix so litellm
        # speaks the OpenAI-compatible protocol that llama-server exposes.
        if self.is_local and not self.model.startswith(("openai/", "ollama/")):
            return f"openai/{self.model}"
        return self.model

    @property
    def display_name(self) -> str:
        if self.is_local:
            return f"{self.model} @ {self.api_base}"
        return self.model


@dataclass
class AppConfig:
    """Top-level application configuration."""

    search: SearchConfig = field(default_factory=SearchConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    db_path: str = "house_hunter.db"
