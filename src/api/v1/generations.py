"""
Image Generation & Asset Delivery Endpoints
Location: src/api/v1/generations.py
"""

from typing import Optional
from fastapi import APIRouter, Depends, Header, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.auth import get_authenticated_api_key
from src.api.rate_limiter import check_rate_limit
from src.api.v1.schemas import GenerationCreateRequest, JobResponse
from src.domain.models import JobStatus
from src.domain.interfaces import AssetStorage
from src.infrastructure.database import get_db_session
from src.infrastructure.models import ApiKeyModel, GenerationJobModel
from src.infrastructure.storage import get_asset_storage
from src.services.generation_service import GenerationService

router = APIRouter(prefix="/images/generations", tags=["Images"])


def get_storage() -> AssetStorage:
    """Dependency provider returning the active configured asset storage adapter."""
    return get_asset_storage()


def map_job_to_dto(job: GenerationJobModel) -> JobResponse:
    """Transforms GenerationJobModel to strongly typed JobResponse DTO."""
    links = {
        "self": f"/api/v1/images/generations/{job.id}",
    }

    is_synthetic = None
    asset = job.__dict__.get("asset")
    if asset:
        links["content"] = f"/api/v1/images/generations/{job.id}/content"
        is_synthetic = asset.is_synthetic
    elif job.provider_used == "mock" or job.provider_requested == "mock":
        is_synthetic = True

    return JobResponse(
        id=job.id,
        status=job.status,
        prompt=job.prompt,
        negative_prompt=job.negative_prompt,
        width=job.width,
        height=job.height,
        steps=job.steps,
        seed=job.seed,
        model=job.model,
        provider_requested=job.provider_requested,
        provider_used=job.provider_used,
        error_code=job.error_code,
        error_message=job.error_message,
        execution_time_ms=job.execution_time_ms,
        peak_vram_mb=job.peak_vram_mb,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        is_synthetic=is_synthetic,
        links=links,
    )


@router.post(
    "",
    summary="Enqueue an asynchronous image generation job",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobResponse,
)
async def create_generation(
    req: GenerationCreateRequest,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    api_key: ApiKeyModel = Depends(check_rate_limit),
    db: AsyncSession = Depends(get_db_session),
):
    """
    Accepts and enqueues an asynchronous image generation request.
    
    Headers:
        Idempotency-Key: Optional deduplication key (up to 128 chars).
    
    Returns:
        HTTP 202 Accepted with Location header pointing to the job resource.
        Returns HTTP 200/202 for idempotent re-submissions.
        Returns HTTP 409 Conflict if same Idempotency-Key is reused with different payload.
    """
    job, is_new = await GenerationService.create_or_get_generation(
        api_key=api_key,
        req=req,
        idempotency_key=idempotency_key,
        db=db,
    )

    location = f"/api/v1/images/generations/{job.id}"
    dto = map_job_to_dto(job)

    # 202 for newly queued or in-flight jobs; 200 for already completed idempotent hits
    status_code = status.HTTP_202_ACCEPTED
    if not is_new and job.status == JobStatus.COMPLETED.value:
        status_code = status.HTTP_200_OK

    return JSONResponse(
        status_code=status_code,
        headers={"Location": location},
        content=dto.model_dump(mode="json"),
    )


@router.get(
    "/{id}",
    summary="Retrieve image generation job status",
    response_model=JobResponse,
)
async def get_generation_status(
    id: str,
    api_key: ApiKeyModel = Depends(get_authenticated_api_key),
    db: AsyncSession = Depends(get_db_session),
):
    """
    Retrieves execution status, provider metadata, and asset links for a job.
    Enforces cross-client isolation (HTTP 403 on unauthorized access).
    """
    job = await GenerationService.get_job_for_client(api_key, id, db)
    return map_job_to_dto(job)


@router.get(
    "/{id}/content",
    summary="Download generated image asset",
)
async def get_generation_content(
    id: str,
    api_key: ApiKeyModel = Depends(get_authenticated_api_key),
    db: AsyncSession = Depends(get_db_session),
    storage: AssetStorage = Depends(get_storage),
):
    """
    Streams the raw generated PNG binary from Content-Addressable Storage.
    Enforces authorization ownership and private cache controls.
    """
    content_bytes, job = await GenerationService.get_asset_bytes_for_client(
        api_key=api_key,
        job_id=id,
        storage=storage,
        db=db,
    )

    headers = {
        "Content-Type": "image/png",
        "Cache-Control": "private, no-transform, max-age=86400",
        "ETag": f'"{job.asset.file_hash}"',
        "Content-Disposition": f'inline; filename="{job.id}.png"',
        "X-Content-Type-Options": "nosniff",
    }

    return Response(
        content=content_bytes,
        media_type="image/png",
        headers=headers,
    )
